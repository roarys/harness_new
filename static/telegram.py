import requests

def send_telegram_message(bot_message="Didnt pass through a message"):
    group_telegram = "-4172209678"
    betfair_bot_id = "7087062050:AAGQRjDVxHoLf5Av-V5LvvZsg-Zn6US3QZM"

    send_text = 'https://api.telegram.org/bot' + betfair_bot_id + '/sendMessage?chat_id=' + group_telegram + \
                '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()

if __name__ == '__main__':
    send_telegram_message('test')