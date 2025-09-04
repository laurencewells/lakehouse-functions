from common.repository import Unity

async def main():
    unity = Unity()
    result = await unity.run_sql_statement_async("select * from `_data`.`tpch`.`dim_customer` limit 10;")
    print(result)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
