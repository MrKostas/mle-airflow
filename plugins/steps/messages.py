from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.models import Variable

def send_telegram_success_message(context):
    telegram_token = Variable.get('6561370081:AAFXM60UHNm9YPg43JFYK6yX1lkPD6wHl44')
    telegram_chat_id = Variable.get('-4224413310')
    hook = TelegramHook(telegram_conn_id='mle_airflow_MrKostas',
                        token = telegram_token,
                        chat_id = telegram_chat_id)
    dag = context['ti'].dag_id
    run_id = context['ti'].run_id
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': telegram_chat_id,
        'text': message
    })

def send_telegram_failure_message(context):
    telegram_token = Variable.get('6561370081:AAFXM60UHNm9YPg43JFYK6yX1lkPD6wHl44')
    telegram_chat_id = Variable.get('-4224413310')
    hook = TelegramHook(telegram_conn_id = 'mle_airflow_MrKostas',
                       token = telegram_token,
                       chat_id = telegram_chat_id)
    dag = context['ti'].dag_id
    run_id = context['ti'].run_id
    tiks = context['ti'].log_url
    
    message = f'Исполнение DAG {dag} с id={run_id} выполнено неверно. Причины ошибки {tiks}'
    hook.send_message({
        'chat_id': telegram_chat_id,
        'text': message
    })