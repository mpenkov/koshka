#!/usr/bin/env python
"""Wrapper for the SSH command.

Allows you to reference EC2 instances via their instance ID as opposed to the
hostname.  The hostname must be the very first parameter for this to work.

Configuration happens via ~/kssh.cfg.
"""
import configparser
import os
import subprocess
import sys

import boto3  # type: ignore


def _config():
    parser = configparser.ConfigParser()
    parser.read(os.path.expanduser('~/kssh.cfg'))
    return parser


def _resolve(host, parser):
    for section in parser.sections():
        try:
            session = boto3.Session(profile_name=section)
            client = session.client('ec2')
            response = client.describe_instances(InstanceIds=[host])
            hostname = response['Reservations'][0]['Instances'][0]['PublicIpAddress']
            return hostname, session.profile_name
        except Exception:
            raise


def main():
    host = sys.argv[1]

    command = sys.argv[:]
    command[0] = 'ssh'

    if host.startswith('i-'):
        parser = _config()
        hostname, profile_name = _resolve(host, parser)
        username = parser[profile_name].get('username')
        command[1] = f'{username}@{hostname}' if username else hostname

        options = parser[profile_name].get('options')
        for opt in options.split(' '):
            command += ['-o', opt]

        identity_file = parser[profile_name].get('identity_file')
        if identity_file:
            command += ['-i', os.path.expanduser(identity_file)]

    subprocess.check_call(command)


if __name__ == '__main__':
    main()
