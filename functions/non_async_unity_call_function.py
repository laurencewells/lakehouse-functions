from common.repository import Unity

def main():
    unity = Unity()
    result = unity.run_sql_statement("select * from `_data`.`tpch`.`dim_customer` limit 100;")
    print(result)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
