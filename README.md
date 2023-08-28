# GPTserver

# 실행 방법

1. 깃배쉬로 azure server에 들어간다.

2. python 코드에서 api key 값을 추가한다.

![image](https://github.com/Git-DDong-Chan/GPTserver/assets/101232265/09e72516-3dc6-4335-9868-b0deb1a8c272)

3. cd kafka

4. sudo docker compose up -d

5. sudo docker compose exec -it kafka kafka-console-producer --topic chatgpt --broker-list kafka:9092
