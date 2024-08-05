""" Enables support for calling the sg-archive cli using `python -m sg_archive`
"""


import sys

import sg_archive.cli

if __name__ == "__main__":
    # prog_name prevents __main__.py from being shown as the command name in the help
    # text. We don't know the exact command the user passed so we provide a generic
    # `python -m sg_archive` command.
    sys.exit(sg_archive.cli.main(prog_name="sg_archive"))
