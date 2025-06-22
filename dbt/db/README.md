Welcome to your new dbt project!

change to a dbt project directory

### Using the starter project

Try running the following commands:
- dbt run
- dbt test
- dbt parse
- dbt docs generate --empty-catalog --no-compile
- dbt run-operation generate_model_yaml --args '{model_names: [raw_block_model]}'
- dbt run-operation generate_model_yaml --args '{model_names: [raw_block_results]}' --quiet > models/spacebox/raw_block_results.yml
- dbt run-operation generate_source --args '{schema_name: spacebox, database_name: spacebox, table_names: [raw_block_results], generate_columns: True }'
- dbt run-operation generate_model_yaml --args '{model_names: [message_event]}' --quiet > models/spacebox/message_event.yml

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
