pip install poetry

# Steps to update your Poetry dependencies

# 1. Remove existing environment (optional, but recommended for a clean start)
poetry env remove --all

# 2. Update your pyproject.toml file with the contents provided

# 3. Install dependencies with specific versions
poetry install

# 4. Verify installed packages
poetry show

# 5. Activate the virtual environment
poetry shell
