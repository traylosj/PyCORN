"""`python -m pycorn find|export ...`: the same as the `pycorn` command (`pycorn.cli`)."""
import sys

from .cli import main

sys.exit(main())
