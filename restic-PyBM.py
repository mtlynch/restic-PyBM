#!/usr/bin/env python3

# restic wrapper and status checker
# Written by Eric Viseur <eric.viseur@gmail.com>, 2021
# Released under MIT license

# v0.1 - 04/02/21 - Initial release
# v0.2 - In progress - Minor fixes, restic auto-update

# ---- imports ----------------------------------------------------------------

import sys
import subprocess
import errno
import yaml
import os
import json
from datetime import datetime, timedelta
from argparse import ArgumentParser

import restic

# ---- constants --------------------------------------------------------------

APPDESC = 'A restic wrapper and Nagios-compliant status checker using a YAML configuration file.  Version 0.2.'
CONFIG_FILE = 'backup.yml'

# ---- create the command line options -----------------------------------------


def create_args():
    parser = ArgumentParser(description=APPDESC)

    parser.add_argument('action', action='store',
                        choices=['run', 'create', 'list', 'prune', 'check'],
                        help='Action to execute.')

    parser.add_argument('repo', action='store', nargs='?', default='ALL_REPOS',
                        help='Repository name, as declared in the configuration file. If omitted, the action is executed on all repos.')

    parser.add_argument("-c", "--config-file", action="store",
                        dest='configFile', default=CONFIG_FILE,
                        help=("Configuration file location. Default [%s]" % CONFIG_FILE))

    parser.add_argument("--full", action='store_true',
                        help='check action:  Verifies the actual snapshots content on top of repository metadata.')

    parser.add_argument("--age", action='store_true',
                        help='check action:  Verify the age of the snapshots.')

    parser.add_argument("--perfdata", action="store_true",
                        help='check action: Outputs Nagios-compliant perfdata metrics')

    parser.add_argument("-v", "--verbose", action='store_true',
                        help='Provide restic output even for successful execution of actions.')

    parser.add_argument("-q", "--quiet", action='store_true',
                        help='Output only error messages.')

    parser.add_argument("-u", "--self-update", action='store_true',
                        dest='selfUpdate', help='Self-update restic before any other action.')

    parser.add_argument("-V", "--use-vault", action='store_true',
                        dest='vault', help='Get the repositories passwords from HashiCorp Vault.')

    args = parser.parse_args()
    return args

# ---- parse the YAML configuration file --------------------------------------


def parse_config(configFile):

  # Check if the config file exists
  if os.path.exists(configFile):
    # Attempt to read the config file contents
    try:
      stream = open(configFile, 'r')
      configValues = yaml.load(stream, Loader=yaml.BaseLoader)

      resticLocation = configValues['restic_binary_location']
      repos = configValues['repos']

      if 'vault' in configValues.keys(): vaultData = configValues['vault']
      else: vaultData = ''

      return [resticLocation, repos, vaultData]
    except:
      print("CRITICAL - Error reading the configuration file %s" %
            configFile)
      exit(2)
  else:
    print("CRITICAL - Configuration file %s does not exist" % configFile)
    exit(2)

# ---- obtain a repository password -------------------------------------------
def get_repo_password(repos, currentRepo, vault = False):
  if vault:
    vaultRead = vault.secrets.kv.v2.read_secret_version(
      path=repos[currentRepo]['key']['path'],
      mount_point=repos[currentRepo]['key']['mountpoint']
    )
    if repos[currentRepo]['location'][0:3] == 'b2:':
      return(vaultRead['data']['data'])
    else:
      return(vaultRead['data']['data']['password'])
  else:
    return(repos[currentRepo]['key'])

# ---- generate the output and ensure the repo is unlocked --------------------
def end_script(returnCode, stdOut, stdErr, successMsg, errorMsg, quiet, verbose):

  # Process the output
  if returnCode == 2:
    print("CRITICAL - %s" % errorMsg)
    print("Output: %s" % stdOut)
    print("Error: %s" % stdErr)
    exit(2)
  else:
    if returnCode == 1:
      if not quiet:
        print("WARNING - %s" % errorMsg)
      if verbose:
        print("Output: %s" % stdOut)
        print("Error: %s" % stdErr)
      exit(1)
    else:
      if not quiet:
        print("OK - %s" % successMsg)
      if verbose:
        print("------------------------------------------------------------------------------")
        print(stdOut)
      exit(0)


# ---- mainline ---------------------------------------------------------------
# -----------------------------------------------------------------------------

# Parse the arguments and read the configuration file
args = create_args()
(resticLocation, repos, vaultData) = parse_config(args.configFile)

# Check if the provided repo exists in the configuration file
if not args.repo in repos.keys() and not args.repo == 'ALL_REPOS':
  print("Repository %s absent from %s" % (args.repo, args.configFile))
  exit(2)

# If requested, self update restic first
if args.selfUpdate:
    try:
        restic.self_update()
    except restic.Error as e:
        print("CRITICAL - restic self-update failed: %s." % str(e))
        exit(2)

# Build a list with the repos to process
reposToProcess = []
if args.repo == 'ALL_REPOS':
  for entry in repos:
    reposToProcess.append(entry)
else:
  reposToProcess.append(args.repo)

# If Vault is to be used, open the connection
if args.vault:
    import hvac
    vault = hvac.Client(url=vaultData['server'])
    vault.auth.approle.login(
      role_id=vaultData['role_id'],
      secret_id=vaultData['secret_id'],
    )

# Initialize accumulation variables used to create the script output messages
successMessageAccumulated = ''
errorMessageAccumulated = ''
stdoutAccumulated = ''
stderrAccumulated = ''
scriptReturnValue = 0

# Run the requested action on all selected repositories
for currentRepo in reposToProcess:

  # Get the repository credentials
  if args.vault: repoCredentials = get_repo_password(repos, currentRepo, vault)
  else: repoCredentials = get_repo_password(repos, currentRepo)

  if repos[currentRepo]['location'][0:3] == 'b2:':
    os.environ.set('B2_ACCOUNT_ID', repoCredentials['keyID'])
    os.environ.set('B2_ACCOUNT_KEY', repoCredentials['applicationKey'])
    os.environ.set('RESTIC_PASSWORD', repoCredentials['password'])
  else:
    os.environ.set('RESTIC_PASSWORD', repoCredentials)

  # If this a duplicate type repo, also get the source repository key
  if 'duplicate' in repos[currentRepo].keys():
    duplicateSource = repos[currentRepo]['duplicate']

    if args.vault: repoCredentials2 = get_repo_password(repos, duplicateSource, vault)
    else: repoCredentials2 = get_repo_password(repos, duplicateSource)
    os.environ.set('RESTIC_PASSWORD2', repoCredentials2)

    # When duplicating we need to invert the password variables 1 and 2
    if args.action == 'run':
      os.environ['RESTIC_PASSWORD2'], os.environ['RESTIC_PASSWORD'] = (
          os.environ['RESTIC_PASSWORD'], os.environ['RESTIC_PASSWORD2']
      )



  # ---- actions execution ----------------------------------------------------

  restic.binary_path = resticLocation
  restic.repository = repos[currentRepo]['location']
  if args.action == 'create':
      repo2 = None
      copyChunkerParams = False
      # If this is a repo that will hold duplicates, set the proper parameters
      if 'duplicate' in repos[currentRepo].keys():
        repo2 = repos[duplicateSource]['location']
        copyChunkerParams = True

      # Create a new restic repo with the infos provided in backup.yml
      restic.init(repo2=repo2, copy_chunker_params=copyChunkerParams)

      # Return the results
      successMessage = ("Repository %s successfully created at location %s" % (currentRepo, repos[currentRepo]['location']))
      errorMessage = ("Error creating repository %s" % repos[currentRepo]['location'])

  if args.action == 'prune':
      # Clean up repo according to provided preservation policy
      restic.forget(group_by='host',
                    keep_within=repos[currentRepo]['max_age'] + 'd',
                    prune=True)
      # Return the results
      successMessage = ("Repository %s clean up successful" % currentRepo)
      errorMessage = ("Error cleaning up repository %s" % currentRepo)

  elif args.action == 'check':
      readData = False

      if args.full:
          readData= True
      errorMessage = ''
      # Check the repository integrity
      if not restic.check(read_data=readData):
          errorMessage = ("Error checking repository %s" % currentRepo)
      else:
          # If requested, check the snapshots age
          if args.age:
              try:
                  snaps = restic.snapshots(group_by='host')

                  # Oldest snapshot is the first one
                  oldestTime = snaps[0]['snapshots'][0]['time']
                  # Newest snapshot is the last one
                  newestTime = snaps[0]['snapshots'][len(
                      snaps[0]['snapshots'])-1]['time']
                  # Convert to Pythonic time structures
                  timeFormat = '%Y-%m-%dT%H:%M:%S'
                  oldestTime = datetime.strptime(oldestTime[0:18], timeFormat)
                  newestTime = datetime.strptime(newestTime[0:18], timeFormat)
                  # Compute snapshots ages versus the current time
                  currentTime = datetime.now()
                  oldDiff = currentTime - oldestTime
                  newDiff = currentTime - newestTime
                  # Check ages versus config
                  if oldDiff > timedelta(days=int(repos[currentRepo]['max_age'])):
                      errorMessage = (
                          "Oldest snapshot on %s is %s old" % (currentRepo, oldDiff))
                  if newDiff > timedelta(days=int(repos[currentRepo]['min_age'])):
                      errorMessage = (
                          "Newest snapshot on %s is %s old" % (currentRepo, newDiff))
                  else:
                      result.stdout = result.stdout + "\n" + \
                          ("Newest snapshot age: %s" % newDiff) + \
                          "\n" + ("Oldest snapshot age: %s" % oldDiff)
              except restic.errors.Error as e:
                  errorMessage = (
                      "Error getting snapshots for repository %s" % currentRepo)

      # Return the results
      successMessage = ("Repository %s is healthy" % currentRepo)
      # errorMessage is already defined

  elif args.action == 'list':
      # List snapshots in the repo
      restic.snapshots(group_by='host')
      # Return the results
      successMessage = ("Snapshot list retreived for repository %s" % currentRepo)
      errorMessage = ("Error listing snapshots on repository %s" % repos[currentRepo]['location'])

  else:
      # If this is a duplicate type repo, we copy snapshots from the source to the destination
      if 'duplicate' in repos[currentRepo].keys():
        restic.repository = repos[duplicateSource]['location'])
        restic.copy(repo2=repos[currentRepo]['location'])

        # Swap the repositories password to enable the unlock
        os.environ.set('RESTIC_PASSWORD', os.environ.get('RESTIC_PASSWORD2'))

      # For a standard repo, create a new snapshot
      else:
        excludes = ['lost+found']
        # Incorporate excludes if present
        if 'excludes' in repos[currentRepo]:
          excludes += repos[currentRepo]['excludes']
        restic.backup(paths=repos[currentRepo]['includes'], exclude_patterns=excludes)

      # Return the results
      successMessage = ("Snapshot successfully created on repository %s" % currentRepo)
      errorMessage = ("Error creating new snapshot on repository %s" % repos[currentRepo]['location'])

  successMessageAccumulated += successMessage + ". "
  errorMessageAccumulated += errorMessage + ". "

  # Ensure the repository is unlocked
  try:
    restic.unlock()
  except restic.errors.Error:
    scriptReturnValue = 1

# Provide the user output
end_script(
  scriptReturnValue,
  stdoutAccumulated,
  stderrAccumulated,
  successMessageAccumulated,
  errorMessageAccumulated,
  args.quiet,
  args.verbose
)
