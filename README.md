# Salt connector for netunicorn 
This is a connector for netunicorn platform to saltstack-managed infrastructure.

## Installation
```bash
pip install netunicorn-connector-salt
```

## Usage
Create a configuration file (see [example](configuration-example.yaml)). Then, add the connector information
to the main configuration file of netunicorn-director-infrastructure module (see corresponding repo).

This connector represents saltstack-managed infrastructure to netunicorn platform.
It works via Salt CherryPy API and requires a corresponding Salt configuration changes.

### Salt configuration
In master configuration file (usually `/etc/salt/master`) specify the following options:
```yaml
# external_auth configuration
external_auth:
  pam:
    remoteusername:
      - .*
      - '@wheel'
      - '@runner'
      - '@jobs'
...
# cherrypy configuration
rest_cherrypy:
  port: your_cherrypy_port
  host: your_cherrypy_host
```

See the Salt's documentation for more details.