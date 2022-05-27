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
docker exec -it -u submituser -w /srv/Experiment data-placement-algo-example-submit-1 bash
python3 example_wf.py -f ds1.txt ds2.txt ds3.txt ds4.txt ds5.tar.gz -o O
```
