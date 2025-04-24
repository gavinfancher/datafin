# DataFin

A Python package for financial data analysis.

## Installation

1. Navigate to the package directory:
```bash
cd datafin-package
```

2. Install the package in editable mode:
```bash
pip install -e .
```

The `-e` flag installs the package in "editable" mode, which means:
- You can import `datafin` from anywhere on your system
- Changes to the package code will be immediately reflected without needing to reinstall
- The package is installed in your current Python environment

## Usage

Once installed, you can import the package from anywhere:
```python
from datafin import your_module
```

## Development

This package is structured as follows:

```
datafin-package/
├── datafin/           # Main package directory
│   ├── __init__.py   # Package initialization
│   └── ...           # Your modules
├── setup.py          # Package configuration
└── README.md         # This file
```

Note: The package name "datafin" is what you use in imports, regardless of the directory name. 