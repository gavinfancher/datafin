# datafin-package

Welcome!


### Install the `datafin` package!

Start by initiating the virtual environment
```bash
python3.12 -m venv datafin-package-venv
source datafin-package-venv/bin/activate
```

While inside the `data-fin` directory, if you run:
```bash
pip install -r requirements.txt
```

The `requirements.txt` should install the `datafin-package` and all of its dependencies

Once installed, you can import the package from anywhere:
```python
from datafin import your_module
```
If you can't import the datafin package, follow these steps:

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