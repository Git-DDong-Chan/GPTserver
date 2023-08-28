import json
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
import openai
import mysql.connector

openai.api_key = 'your-key'

mydb = mysql.connector.connect(
    host='20.249.88.211',
    port=3306,
    user='rbmasteruser',
    password='1234',
    database="rb",
)

def init_consumer():
    conf = {'bootstrap.servers': "20.249.88.211:29092",
            'group.id': "chatgpt",
            'auto.offset.reset': 'smallest'}

    return Consumer(conf)



def request_to_chatGPT(data):
    content = data.get('content')

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that recommends books."},
            {"role": "user", "content": f"{content} 이 감정에 어울리는 혹은 달래줄만한 책 10권만 추천해줘.\n"
                                        "책은 교보문고 사이트에 있는 책들로 추천해줘.\n"
                                        "기본적으로 답변은 단답형으로 <\"책 제목\" - 저자> 형식으로 답변해줘 \n"
             }
        ]
    )

    return response["choices"][0]["message"]["content"]

def save_db(title, author, user_id):
    mycursor = mydb.cursor()
    sql = "INSERT INTO book (title, author, checks, user_id) VALUES (%s, %s, %s, %s)"

    try:
        mycursor.execute(sql, (title, author, 0, user_id))
        mydb.commit()
    except Exception as e:
        print("An error occurred:", e)


def msg_process(msg):
    msg_data = json.loads(msg)

    command = msg_data.get('command')

    if command is None:
        print("there is no command")
        return

    if command == 'request_chatgpt':
        command_data = msg_data.get('data')
        user_id = command_data.get('user_id')  # user_id 추출

        if command_data is None or user_id is None:
            return

        response_content = request_to_chatGPT(command_data)
        response_lines = response_content.split('\n')

        for line in response_lines:
            title_start = line.find("\"")
            title_end = line.rfind("\"")
            if title_start != -1 and title_end != -1:
                title = line[title_start + 1:title_end].strip()
                author = line[title_end + 3:].strip()
                save_db(title, author, user_id)
                print(title, author, user_id)
            else:
                print("Could not parse title and author")

        return

    print(f"the command is not valid: {command}")
    return

def consume_message(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:  # 무한 루프로 변경하여 자동 재시작
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg.value())
    except Exception as e:
        print("An error occurred:", e)
        time.sleep(5)  # 일시적인 오류 발생 시 잠시 대기 후 다시 시작
    finally:
        consumer.close()

if __name__ == '__main__':
    while True:  # 무한 루프로 코드 실행을 지속적으로 시도
        try:
            consumer = init_consumer()
            consume_message(consumer, ['chatgpt'])
        except Exception as e:
            print("An error occurred:", e)
            time.sleep(5)  # 잠시 대기 후 다시 시작