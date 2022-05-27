# Start/Stop/Destroy

```bash
docker compose up
docker compose stop
docker compose down
```

# HTCondor Custom Config

Define custom configuration in config/\<role\>/*.conf files.

# Change Pool Password

```bash
# Current Password: PegasusWMS123*
condor_store_cred -c add -d passwd/POOL
Enter password:
```

# Running Pegasus Commands

```
docker exec -it -u submituser htcondor-docker-compose-submit-1 bash
```
