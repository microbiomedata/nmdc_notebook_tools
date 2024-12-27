# nmdc_notebook_tools
A library designed to simplify various research tasks for users looking to leverage the NMDC (National Microbiome Data Collaborative) APIs. The library provides a collection of general-purpose functions that facilitate easy access, manipulation, and analysis of microbiome data.

# Usage
Until this available through pip, the best usage will be to clone the repo and use like this example:
```python
from nmdc_notebook_tools.collection import Collection

# Create an instance of the module
collection_client = Collection()
# Use the variable to call the available functions
collection_client.get_collection_by_id("biosample", "id")
```
I would recommend periodically runnning `git pull` to get the latest updates.

# Installation
Note: nmdc_notebook_tools will eventually be available at `pip install nmdc_notebook_tools` but is still in development.

# Documentation
Documentation about available functions and helpful usage notes can be found at https://microbiomedata.github.io/nmdc_notebook_tools/.
