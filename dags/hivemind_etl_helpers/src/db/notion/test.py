from extractor import NotionExtractor

notion_extractor = NotionExtractor(
    "secret_kPH3bgR0iMYLqmX9KkPi9wRRueGFjEvKAIlXf2G9wj"
)
page_ids = ["6a3c20b6861145b29030292120aa03e6",
            "e479ee3eef9a4eefb3a393848af9ed9d"]
database_ids = ["dadd27f1dc1e4fa6b5b9dea76858dabe"]

results = notion_extractor.extract(page_ids=page_ids,
                                   database_ids=database_ids
                                   )
print(results)