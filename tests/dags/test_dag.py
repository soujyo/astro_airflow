from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dag_bag.import_errors) == 0, "No Import Failures"


def test_no_import_errors_check_with_id():
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='assignment_snowflake')
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    assert dag is not None


def test_dag_structure():
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='assignment_snowflake')
    target_keys = {'start', 'snowflake_create_raw_tables', 'snowflake_create_stage_tables',
                   'snowflake_load_stage_tables_compact', 'snowflake_load_stage_tables_exploded',
                   'loading_aggregated_views_snowflake'}
    print(dag.task_dict.keys())
    assert dag.task_dict.keys() == target_keys


def test_retries_present():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    for dag in dag_bag.dags:
        retries = dag_bag.dags[dag].default_args.get('retries', [])
        print(retries)
        error_msg = 'Retries not set to 1 for DAG {id}'.format(id=dag)
        assert retries == 1, error_msg


test_no_import_errors_check_with_id()
test_dag_structure()
test_retries_present()
test_no_import_errors()
