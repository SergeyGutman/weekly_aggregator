import os
import sys
import csv
from datetime import datetime, timedelta


def check_day_aggr_file(input_date):
    #проверка наличия агрегированного файла за день из директории intermediate, если нет, то создается
    if not os.path.exists(f'intermediate/{input_date}_aggr.csv'):
        aggregate_user_actions_for_day(input_date)


# агрегирование данных за день
def aggregate_user_actions_for_day(input_date):
    # чтение данных из CSV файла -> вставить проверку на наличие файла и выдавать ошибку если нету
    with open(f'input/{input_date}.csv', mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=('email', 'action', 'dt'))
        actions = list(reader)
    aggregated = {}
    # заполнение словаря данными
    for action in actions:
        email = action['email']
        action_type = action['action']
        aggregated[email] = aggregated.get(email, {
            'CREATE': 0,
            'READ': 0,
            'UPDATE': 0,
            'DELETE': 0
        })
        aggregated[email][action_type] += 1

    # генерируем имя выходного файла
    output_file = os.path.join(f'intermediate/{input_date}_aggr.csv')

    # записываем результаты в CSV файл -> вставить проверку на наличие директории intermediate
    with open(output_file, mode='w', newline='') as csvfile:
        fieldnames = [
            'email', 'create_count', 'read_count', 'update_count',
            'delete_count'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for email, counts in aggregated.items():
            writer.writerow({
                'email': email,
                'create_count': counts['CREATE'],
                'read_count': counts['READ'],
                'update_count': counts['UPDATE'],
                'delete_count': counts['DELETE']
            })


# аггрегирование данных за неделю
def aggregate_user_actions(input_date):
    # определяем даты для последней недели
    end_date = datetime.strptime(target_date, '%Y-%m-%d')
    start_date = end_date - timedelta(days=7)
    list_days = []
    # генерируем промежуточные файлы за день, если они отсутсвуют, в папке intermediate
    for i in range(7):
        current_date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        list_days.append(current_date)
        check_day_aggr_file(current_date)
    week_aggregated = {}
    # считываем из агрегированных по дням файлов счётчики crud и агрегируем их за неделю 
    for day in list_days:
        with open(f'intermediate/{day}_aggr.csv', mode='r',
                  newline='') as csvfile:
            reader = csv.DictReader(csvfile,
                                    fieldnames=('email', 'create_count',
                                                'read_count', 'update_count',
                                                'delete_count'))
            for row in reader:
                email = row['email']
                create_count = int(row['create_count'])
                read_count = int(row['read_count'])
                update_count = int(row['update_count'])
                delete_count = int(row['delete_count'])
                week_aggregated[email] = week_aggregated.get(
                    email, {
                        'create_count': 0,
                        'read_count': 0,
                        'update_count': 0,
                        'delete_count': 0
                    })
                week_aggregated[email]['create_count'] += create_count
                week_aggregated[email]['read_count'] += read_count
                week_aggregated[email]['update_count'] += update_count
                week_aggregated[email]['delete_count'] += delete_count
        # записываем результаты в csv файл в папку output
        output_file = os.path.join(f'output/{input_date}.csv')
        with open(output_file, mode='w', newline='') as csvfile:
            fieldnames = [
                'email', 'create_count', 'read_count', 'update_count',
                'delete_count'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            for email, counts in week_aggregated.items():
                writer.writerow({
                    'email': email,
                    'create_count': counts['create_count'],
                    'read_count': counts['read_count'],
                    'update_count': counts['update_count'],
                    'delete_count': counts['delete_count']
                })


if __name__ == "__main__":
    target_date = sys.argv[1]
    aggregate_user_actions(target_date)