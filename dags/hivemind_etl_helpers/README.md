# Hivemind ETL

In this repository we're writing the data etl process scripts for hivemind bot.

## How to

For now, the scripts are focused on having discord data within mongodb and will store the embdeddings in postgress. To start the scripts for mongodb discord data

```python3
python discord_mongo_etl.py [guild_id]
```

Notes: Please replace [guild_id] with your guild id.
