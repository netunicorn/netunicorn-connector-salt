# Salt connector for netunicorn 
This is a connector for netunicorn platform to saltstack-managed infrastructure.

## Installation
```bash
pip install netunicorn-connector-salt
```

## Usage
Create a configuration file (see [example](configuration-example.yaml)). Then, add the connector information
to the main configuration file of netunicorn-director-infrastructure module (see corresponding repo).

This connector is supposed to run on the salt master and have access to the salt API. It will represent 
saltstack-managed infrastructure to netunicorn platform.