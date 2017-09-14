import resources

CDM_TABLES = set([r['table_name'] for r in resources.cdm_csv()])
CDM_FILES = map(lambda t: t + '.csv', CDM_TABLES)
RESULT_CSV = 'result.csv'
ERRORS_CSV = 'errors.csv'
WARNINGS_CSV = 'warnings.csv'
LOG_JSON = 'log.json'
IGNORE_LIST = [RESULT_CSV, ERRORS_CSV, WARNINGS_CSV]