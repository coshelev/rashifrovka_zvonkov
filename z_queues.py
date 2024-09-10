#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import re
import datetime
import sys

from logwrite import logwrite


# Покатили переменные и константы
logpath = '/var/log/aster-crm.log'
# logpath = './aster-crm.log'

dbuser = 'sergoid'
dbpwd = 'Lqw743iFg'
dbname = 'asterisk'
dbhost = 'localhost'
encode_query = "SET NAMES `utf8`"

avail_in = []
ufield_in = []
avail_out = []
ufield_out = []
last_id = ''
current_src = ''
transfer_count = []  # Список id разобранных ATXFER из cel (чтобы не задваивались)
temp_table = []
ringall_queues = ['ECC-1', ]
is_ringall = 0

regexp1 = re.compile(r'\D+')
regexp2 = re.compile(r'"(\w+)"\s+<(.+)>')
regexp4 = re.compile(r'^out-(\d+?\.\d+?)$')
regexp6 = re.compile(r'^QueueRoutine,(.+)$')
regexp_sip = re.compile(r'^.+?/(\d+?)-.+$')

# Выбираем из z_queue_last userfield для неоконченных в прошлый запуск скрипта звонков
query00 = "SELECT linkedid FROM z_queue_last WHERE status = %s"
# Выбираем userfield исходящих звонков для обработки
query01 = "SELECT DISTINCT userfield FROM cdr WHERE calldate LIKE %s AND userfield LIKE 'out%%' ORDER BY userfield"
# Выбираем userfield входящих звонков для обработки
query02 = """SELECT DISTINCT userfield FROM cdr WHERE calldate like %s AND userfield NOT IN ('', 'HOLIDAY', 'BLOCKED', 'INVALID-NUMBER')
            AND userfield NOT LIKE 'out%%' AND userfield NOT LIKE 'test%%' ORDER BY userfield"""
# Select all Incoming/Outgoing "linkedid" for today, чтобы по отсутствию в ней находить необработанные
query03 = "SELECT DISTINCT linkedid FROM z_queue_events WHERE eventtime LIKE %s"
query04 = "SELECT DISTINCT linkedid FROM z_outcall_events WHERE eventtime LIKE %s"
# Выбираем данные для записи события START (uniqueid нужен, чтобы не задвоить записи)
query11 = "SELECT calldate, clid, src, dst, lastapp, uniqueid FROM cdr WHERE userfield = %s ORDER BY uniqueid, calldate"
# Выбираем данные для записи события START (uniqueid нужен, чтобы не задвоить записи)
query12 = "SELECT calldate, clid, src, dst FROM cdr WHERE uniqueid = %s"
# Выбираем данные для записи события START OUT (uniqueid нужен, чтобы не задвоить записи)
query14 = """SELECT calldate, uniqueid, clid, src, dst, channel, disposition FROM cdr
            WHERE linkedid = %s ORDER BY sequence"""
# Выбираем количество записей с определенным linkedid для проверки - отдельный звонок или нет
query21 = "SELECT COUNT(*) FROM cel WHERE linkedid = %s"
# Ищем событие окончания звонка. Если такого нет, то считаем звонок еще неоконченным
query22 = "SELECT eventtime FROM cel WHERE linkedid = %s AND eventtype='LINKEDID_END'"
# Выбираем из queue_log все события для очереди с определенным callid
query3 = "SELECT * FROM queue_log WHERE callid = %s AND (event in (%s,%s,%s,%s,%s,%s,%s,%s,%s)) ORDER BY time"
# Находим таймаут заданный для очереди, чтобы определить RINGNOANSWER или BUSY
query31 = "SELECT timeout FROM queues WHERE name = %s"
# Для вложенных очередей находим их callid
query4 = "SELECT DISTINCT callid FROM queue_log WHERE queuename=%s AND callid>%s ORDER BY callid"
# Проверяем, действительно ли часть звонка (очередь) является вложенной для основного звонка
query5 = "SELECT COUNT(*) FROM cel WHERE uniqueid = %s AND linkedid = %s"
# После CONNECT в очереди проверяем, был ли перехват, или вызов был принят адресатом
query61 = "SELECT eventtime, extra FROM cel WHERE exten IN (%s, %s) AND linkedid = %s AND eventtype = 'PICKUP'"
# Если в предыдущем запросе (query61) был перехват, то получаем время и номер перехватившего
# 2018-01-26 query62 = "SELECT eventtime, channame FROM cel WHERE eventtype = 'ANSWER' AND uniqueid = %s"  # если был перехват
# После трансфера проверям, был ли перехват, или вызов был принят адресатом
query71 = """SELECT eventtype, eventtime, channame, extra FROM cel WHERE eventtype IN ('PICKUP', 'ANSWER', 'HANGUP')
            AND eventtime >= %s AND cid_num = %s  AND linkedid = %s ORDER BY eventtime"""
# Ищем причину неответа после BLINDXFER (Failed, BUSY, NO ANSWER)
query72 = "SELECT disposition FROM cdr WHERE dstchannel = %s AND linkedid = %s"
# Выбираем все оставшиеся BLINDTRANSFER and ATTENDEDTRANSFER после выхода из очереди
query8 = """SELECT id, eventtype, eventtime, cid_name, cid_num, extra FROM cel WHERE linkedid = %s
           AND eventtime > %s AND eventtype IN ('BLINDTRANSFER', 'ATTENDEDTRANSFER')"""
#
query81 = "SELECT id, extra FROM cel WHERE eventtype = %s AND eventtime >= %s AND linkedid = %s"
# Выбираем события BRIDGE_ENTER для того, чтобы найти номер, на который сделан ATXFER (для очереди)
query82 = "SELECT exten, extra FROM cel WHERE eventtype = 'BRIDGE_ENTER' AND linkedid = %s AND appname = ''"
# Выбираем события BRIDGE_ENTER для того, чтобы найти номер, на который сделан ATXFER (не из очереди)
query83 = "SELECT exten, extra FROM cel WHERE eventtype = 'BRIDGE_ENTER' AND linkedid = %s AND peer = ''"
# В случае bridge-app ATXFER, находим dst
query84 = "SELECT dst FROM cdr WHERE channel = %s AND linkedid = %s"
# Выбираем все оставшиеся BLINDTRANSFER and ATTENDEDTRANSFER после выхода из очереди для COMPLETEAGENT
query85 = """SELECT id, eventtype, eventtime, cid_num, extra FROM cel WHERE linkedid = %s
           AND eventtime < %s AND eventtype IN ('BLINDTRANSFER', 'ATTENDEDTRANSFER')"""
# Проверяем, не в очередь ли трансфер
query9 = "SELECT appdata FROM extensions WHERE exten=%s AND appdata LIKE 'QueueRoutine%%'"
# Находим callid для разбора очередей в исходящих звонках
# query90 = "SELECT DISTINCT(uniqueid) FROM cdr WHERE linkedid = %s and uniqueid <> %s ORDER BY uniqueid"
query90 = """SELECT DISTINCT(callid) FROM asterisk.queue_log where callid in (
    SELECT * FROM
    (SELECT DISTINCT(uniqueid) FROM asterisk.cdr WHERE linkedid = %s AND uniqueid <> %s)
    AS subquery) ORDER BY callid"""

# Находим номер очереди по её имени
query101 = "SELECT exten FROM extensions WHERE appdata LIKE %s"

# Заполняем нашу таблицу событий
query109 = """INSERT INTO z_outcall_events (`id`,`linkedid`,`eventtime`,`src`,`dst`,`event`,`data`)
           VALUES ('0',%s,%s,%s,%s,%s,%s)"""
query110 = """INSERT INTO z_queue_events (`id`,`linkedid`,`eventtime`,`src`,`dst`,`event`,`data`)
           VALUES ('0',%s,%s,%s,%s,%s,%s)"""
# Заполняем нашу таблицу хвостов
query111 = "INSERT INTO z_queue_last (`id`,`linkedid`,`status`) VALUES ('0',%s,%s)"

# Процедуры в неизвестном пока количестве


def queue_log_read(callid, maincallid, tablename):
    """Разбираем прохождение звонка в очереди. Предусмотрена рекурсия на случай вложенных очередей
     1. Выбираем по очереди все события RINGNOANSWER, CONNECT, COMPLETE..., TRANSFER, ABANDON
     2. Для события CONNECT проверяем имя агента (предполагаем, что телефоны - это цифровые значения,
        а при наличии хотя бы одной буквы в имени агента предполагаем, что это вложенная очередь), и если
        в качестве агента имеем вложенную очередь - запускаем функцию рекурсивно
     3. Для события CONNECT в случае, если это не переход во вложенную очередь, проверяем не был ли
        этот вызов перехвачен
        Тоже делаем и для события TRANSFER, ибо после перевода вызова, он может быть перехвачен"""

    global transfer_count
    global current_src
    global is_ringall
    cur_q = con.cursor()
    cur_q.execute(query3,
                  [callid, 'ENTERQUEUE', 'RINGNOANSWER', 'CONNECT', 'COMPLETECALLER', 'COMPLETEAGENT', 'ABANDON',
                   'BLINDTRANSFER', 'ATTENDEDTRANSFER', 'EXITWITHTIMEOUT'])
    result_q = cur_q.fetchall()
    for row_q in result_q:
        # time_q = str(row_q[0])[0:19]
        time_q = str(row_q[0])
        # Чтобы в нашей таблице записывалось событие END для случая, когда вызов в очереди не принят,
        # надо поймать ANSWER при попадании в очередь, и актуализировать data_lh[0] и data_lh[1]
        if row_q[4] == 'ENTERQUEUE':
            insert_event(tablename, maincallid, time_q, row_q[2], row_q[3], row_q[4], row_q[7])
            if row_q[2] in ringall_queues:
                is_ringall = 1
            else:
                is_ringall = 0

        elif row_q[4] == 'ABANDON':
            if current_src == '':
                insert_event(tablename, maincallid, time_q, row_q[2], row_q[3], row_q[4], row_q[5])
            else:
                insert_event(tablename, maincallid, time_q, row_q[2], current_src, row_q[4], row_q[5])

        elif row_q[4] == 'CONNECT' and regexp1.search(row_q[3]):
            # Если TRUE , значит это вложенная очередь, обрабатываем
            # Событие 'CONNECT' в нашу таблицу не пишем (по договоренности с Лехой)
            # сохраним значение row_q[6], оно нам пригодится при отлове трансфера в asterisk 11.7.0
            #            data_2_field = row_q[6]
            cur_sq = con.cursor()
            cur_sq.execute(query4, [row_q[3], callid])
            for j in range(cur_sq.rowcount):
                result_sq = cur_sq.fetchone()
                if check_uniqueid(result_sq[0], maincallid) == 1:
                    queue_log_read(result_sq[0], maincallid, tablename)
                    break
                else:
                    continue
            cur_sq.close()
        elif row_q[4] == 'CONNECT' and not regexp1.search(row_q[3]):
            # Кто-то принял вызов, проверяем, не перехват ли это
            # Событие 'CONNECT' в нашу таблицу не пишем (по договоренности с Лехой)
            # сохраним значение row_q[6], оно нам пригодится при отлове трансфера в asterisk 11.7.0
            #            data_2_field = row_q[6]
            cur_pickup = con.cursor()
            cur_pickup.execute(query61, [row_q[3], check_number(row_q[2]), maincallid])
            if cur_pickup.rowcount != 0:
                result_pickup = cur_pickup.fetchone()
                pickup_dict = eval(result_pickup[1])
                # Тут номер брать регуляркой из pickup_dict['pickup_channel']
                if_match = regexp_sip.match(pickup_dict['pickup_channel'])
                pickup_dst = ''
                if if_match:
                    pickup_dst = if_match.group(1)
                insert_event(tablename, maincallid, str(result_pickup[0]), row_q[3], pickup_dst,
                             'PICKUP', '')
                current_src = pickup_dst
            else:
                # Если не было перехвата, пишем тут событие ANSWER.
                # Ищем его в cel по uniqueid, из него берем данные для data_lh[0], data_lh[1]

                # -== 2018-01-26 ==-
#                cur_pickup.execute(query62, [row_q[6]])
#                if cur_pickup.rowcount != 0:
#                    result_pickup = cur_pickup.fetchone()
#                    insert_event(tablename, maincallid, str(result_pickup[0]), row_q[3], row_q[3], 'ANSWER', '')
                    insert_event(tablename, maincallid, time_q, row_q[3], row_q[3], 'ANSWER', '')
                # -== 2018-01-26 ==-
            cur_pickup.close()

        elif row_q[4] == 'BLINDTRANSFER':  # Очередь заканчивается трансфером
            qname = check_transfer(row_q[5])  # проверяем, не в очередь ли трансфер
            if current_src == '':
                current_src = row_q[3]
            if qname != '':
                insert_event(tablename, maincallid, time_q, current_src, row_q[5], 'B-TRANSFER', '')
                current_src = ''
                cur_attr = con.cursor()
                cur_attr.execute(query81, ['BLINDTRANSFER', time_q[0:19], maincallid])
                if cur_attr.rowcount != 0:
                    result_attr = cur_attr.fetchone()
                    transfer_count.append(result_attr[0])
                cur_sq = con.cursor()
                cur_sq.execute(query4, [qname, callid])
                for j in range(cur_sq.rowcount):
                    result_sq = cur_sq.fetchone()
                    if check_uniqueid(result_sq[0], maincallid) == 1:
                        queue_log_read(result_sq[0], maincallid, tablename)
                        break
                    else:
                        continue
                cur_attr.close()
                cur_sq.close()

            else:
                insert_event(tablename, maincallid, time_q, current_src, row_q[5], 'B-TRANSFER', '')
                current_src = ''
                cur_attr = con.cursor()
                cur_attr.execute(query81, ['BLINDTRANSFER', time_q[0:19], maincallid])
                if cur_attr.rowcount != 0:
                    result_attr = cur_attr.fetchone()
                    transfer_count.append(result_attr[0])
                find_pickup_after_transfer(tablename, maincallid, time_q, row_q[5], 'BLINDXFER')

                cur_blindtr = con.cursor()  # Выбираем все оставшиеся TRANSFER после выхода из очереди
                cur_blindtr.execute(query8, [maincallid, time_q])
                if cur_blindtr.rowcount != 0:
                    result_blindtr = cur_blindtr.fetchall()
                    cur_subattr = con.cursor()
                    for row_blindtr in result_blindtr:
                        # Если трансфер в очередь, то пишем соответствующее событие и прерываем
                        # дальнейшую обработку трансферов
                        if row_blindtr[1] == 'BLINDTRANSFER' and row_blindtr[0] not in transfer_count:
                            tr_dict = eval(row_blindtr[5])
                            transfer_dst = tr_dict['extension']
                            if check_transfer(transfer_dst) != '':
                                transfer_count.append(row_blindtr[0])
                                insert_event(tablename, maincallid, str(row_blindtr[2]), row_blindtr[3],
                                             transfer_dst, 'B-TRANSFER', '')
                                break
                            if row_blindtr[3] != '':
                                transfer_count.append(row_blindtr[0])
                                insert_event(tablename, maincallid, str(row_blindtr[2]), row_blindtr[3],
                                             transfer_dst, 'B-TRANSFER', '')
                                find_pickup_after_transfer(tablename, maincallid, str(row_blindtr[2]),
                                                           transfer_dst, 'BLINDXFER')
                        elif row_blindtr[1] == 'ATTENDEDTRANSFER':
                            tr_dict = eval(row_blindtr[5])
                            attr_src = row_blindtr[4]
                            attr_dst = ''
                            transfer_count.append(row_blindtr[0])
                            if 'bridge2_id' in tr_dict:  # Если это bridge-bridge ATXFER
                                tr_bridge2_id = tr_dict['bridge2_id']
                                cur_subattr.execute(query83, [maincallid])
                                if cur_subattr.rowcount != 0:
                                    result_subattr = cur_subattr.fetchall()
                                    for subattr in result_subattr:
                                        attr_dict = eval(subattr[1])
                                        if attr_dict['bridge_id'] == tr_bridge2_id:
                                            attr_dst = subattr[0]
                                if check_transfer(attr_dst) != '':
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src, attr_dst,
                                                 'A-TRANSFER', '')
                                    break
                                else:
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src, attr_dst,
                                                 'A-TRANSFER', '')
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_dst, attr_dst,
                                                 'ANSWER', '')
                            elif 'app' in tr_dict:  # Если это bridge-app ATXFER
                                cur_subattr.execute(query84, [tr_dict['channel2_name'], tr_dict['channel2_uniqueid']])
                                if cur_subattr.rowcount != 0:
                                    result_subattr = cur_subattr.fetchone()
                                    attr_dst = result_subattr[0]
                                if check_transfer(attr_dst) != '':
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                 attr_dst, 'A-TRANSFER', '')
                                    break
                                else:
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                 attr_dst, 'A-TRANSFER', '')
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_dst,
                                                 attr_dst, 'ANSWER', '')
                                # Значит это bridge-app ATXFER, и нам надо найти channel2_name, и по нему из cdr
                                # найти dst (Select dst from cdr Where channel = channel2_name)
                                # Тут мы подразумеваем, что все очереди и трансферы за их пределами закончились,
                                # и ловим последний hangup
                else:
                    pass
                cur_blindtr.close()

        elif row_q[4] == 'ATTENDEDTRANSFER':  # Очередь заканчивается трансфером
            if current_src == '':
                current_src = row_q[3]
            tr_bridge_name = row_q[6]
            tr_bridge1_name = ''
            at_dst = ''
            cur_attr = con.cursor()
            cur_attr.execute(query81, ['ATTENDEDTRANSFER', time_q[0:19], maincallid])
            if cur_attr.rowcount != 0:  # Находим bridge_name для dst для ATXFER
                result_attr = cur_attr.fetchall()
                for attr in result_attr:
                    at_dict = eval(attr[1])
                    if at_dict['bridge1_id'] == tr_bridge_name and 'bridge2_id' in at_dict:
                        tr_bridge1_name = at_dict['bridge2_id']
                        transfer_count.append(attr[0])
                        break
            cur_attr.execute(query82, [maincallid])  # Находим, куда был сделан ATXFER
            if cur_attr.rowcount != 0:
                result_attr = cur_attr.fetchall()
                for attr in result_attr:
                    at_dict = eval(attr[1])
                    tr_bridge2_name = at_dict['bridge_id']
                    if tr_bridge1_name != '' and tr_bridge2_name == tr_bridge1_name:
                        at_dst = attr[0]
            if at_dst != '':
                qname = check_transfer(at_dst)  # проверяем, не в очередь ли трансфер
                if qname != '':
                    insert_event(tablename, maincallid, time_q, current_src, at_dst, 'A-TRANSFER', '')
                    current_src = ''
                    cur_sq = con.cursor()
                    cur_sq.execute(query4, [qname, callid])
                    for j in range(cur_sq.rowcount):
                        result_sq = cur_sq.fetchone()
                        if check_uniqueid(result_sq[0], maincallid) == 1:
                            queue_log_read(result_sq[0], maincallid, tablename)
                            break
                        else:
                            continue
                    cur_sq.close()

                else:
                    insert_event(tablename, maincallid, time_q, current_src, at_dst, 'A-TRANSFER', '')
                    current_src = ''
                    find_pickup_after_transfer(tablename, maincallid, time_q, at_dst, 'ATXFER')

                    cur_blindtr = con.cursor()  # Выбираем все оставшиеся TRANSFER после выхода из очереди
                    cur_blindtr.execute(query8, [maincallid, time_q])
                    if cur_blindtr.rowcount != 0:
                        result_blindtr = cur_blindtr.fetchall()
                        cur_subattr = con.cursor()
                        for row_blindtr in result_blindtr:
                            # Если трансфер в очередь, то пишем соответствующее событие и прерываем
                            # дальнейшую обработку трансферов
                            if row_blindtr[1] == 'BLINDTRANSFER' and row_blindtr[0] not in transfer_count:
                                tr_dict = eval(row_blindtr[5])
                                transfer_dst = tr_dict['extension']
                                if check_transfer(transfer_dst) != '':
                                    transfer_count.append(row_blindtr[0])
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), row_blindtr[3],
                                                 transfer_dst, 'B-TRANSFER', '')
                                    break
                                if row_blindtr[3] != '':
                                    transfer_count.append(row_blindtr[0])
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), row_blindtr[3],
                                                 transfer_dst, 'B-TRANSFER', '')
                                    find_pickup_after_transfer(tablename, maincallid, str(row_blindtr[2]),
                                                               transfer_dst, 'BLINDXFER')
                            elif row_blindtr[1] == 'ATTENDEDTRANSFER' and row_blindtr[0] not in transfer_count:
                                tr_dict = eval(row_blindtr[5])
                                attr_src = row_blindtr[4]
                                attr_dst = ''
                                transfer_count.append(row_blindtr[0])
                                if 'bridge2_id' in tr_dict:  # Если это bridge-bridge ATXFER
                                    tr_bridge2_id = tr_dict['bridge2_id']
                                    cur_subattr.execute(query82, [maincallid])
                                    if cur_subattr.rowcount != 0:
                                        result_subattr = cur_subattr.fetchall()
                                        for subattr in result_subattr:
                                            attr_dict = eval(subattr[1])
                                            if attr_dict['bridge_id'] == tr_bridge2_id:
                                                attr_dst = subattr[0]
                                    if check_transfer(attr_dst) != '':
                                        insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        break
                                    else:
                                        insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        insert_event(tablename, maincallid, str(row_blindtr[2]), attr_dst,
                                                     attr_dst, 'ANSWER', '')
                                elif 'app' in tr_dict:  # Если это bridge-app ATXFER
                                    cur_subattr.execute(query84, [tr_dict['channel2_name'],
                                                                  tr_dict['channel2_uniqueid']])
                                    if cur_subattr.rowcount != 0:
                                        result_subattr = cur_subattr.fetchone()
                                        attr_dst = result_subattr[0]
                                    if check_transfer(attr_dst) != '':
                                        insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        break
                                    else:
                                        insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        insert_event(tablename, maincallid, str(row_blindtr[2]), attr_dst,
                                                     attr_dst, 'ANSWER', '')

                                    # Значит это bridge-app ATXFER, и нам надо найти channel2_name, и по нему из cdr
                                    # найти dst (Select dst from cdr Where channel = channel2_name)
                                    # Тут мы подразумеваем, что все очереди и трансферы за их пределами закончились,
                                    # и ловим последний hangup
                    else:
                        pass
                    cur_blindtr.close()

        elif row_q[4] == 'COMPLETEAGENT':
            # Тут может оказаться Transfer в случае, если это был трансфер не в очередь,
            # и он был перехвачен, т.е. проверяем на трансферы

            cur_blindtr = con.cursor()  # Выбираем все оставшиеся TRANSFER после выхода из очереди
            cur_blindtr.execute(query85, [maincallid, time_q])
            if cur_blindtr.rowcount != 0:
                result_blindtr = cur_blindtr.fetchall()
                cur_subattr = con.cursor()
                for row_blindtr in result_blindtr:
                    # Если трансфер в очередь, то пишем соответствующее событие и прерываем
                    # дальнейшую обработку трансферов
                    if row_blindtr[0] not in transfer_count:
                        if row_blindtr[1] == 'BLINDTRANSFER':
                            tr_dict = eval(row_blindtr[4])
                            transfer_dst = tr_dict['extension']
                            if check_transfer(transfer_dst) != '':
                                transfer_count.append(row_blindtr[0])
                                insert_event(tablename, maincallid, str(row_blindtr[2]), row_blindtr[3],
                                             transfer_dst, 'B-TRANSFER', '')
                                break
                            if row_blindtr[3] != '':
                                transfer_count.append(row_blindtr[0])
                                insert_event(tablename, maincallid, str(row_blindtr[2]), row_blindtr[3],
                                             transfer_dst, 'B-TRANSFER', '')
                                find_pickup_after_transfer(tablename, maincallid, str(row_blindtr[2]),
                                                           transfer_dst, 'BLINDXFER')
                        elif row_blindtr[1] == 'ATTENDEDTRANSFER':
                            tr_dict = eval(row_blindtr[4])
                            attr_src = row_blindtr[3]
                            attr_dst = ''
                            transfer_count.append(row_blindtr[0])
                            if 'bridge2_id' in tr_dict:  # Если это bridge-bridge ATXFER
                                tr_bridge2_id = tr_dict['bridge2_id']
                                cur_subattr.execute(query82, [maincallid])
                                if cur_subattr.rowcount != 0:
                                    result_subattr = cur_subattr.fetchall()
                                    for subattr in result_subattr:
                                        attr_dict = eval(subattr[1])
                                        if attr_dict['bridge_id'] == tr_bridge2_id:
                                            attr_dst = subattr[0]
                                if check_transfer(attr_dst) != '':
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                 attr_dst, 'A-TRANSFER', '')
                                    break
                                else:
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                 attr_dst, 'A-TRANSFER', '')
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_dst,
                                                 attr_dst, 'ANSWER', '')
                            elif 'app' in tr_dict:  # Если это bridge-app ATXFER
                                cur_subattr.execute(query84, [tr_dict['channel2_name'], tr_dict['channel2_uniqueid']])
                                if cur_subattr.rowcount != 0:
                                    result_subattr = cur_subattr.fetchone()
                                    attr_dst = result_subattr[0]
                                if check_transfer(attr_dst) != '':
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                 attr_dst, 'A-TRANSFER', '')
                                    break
                                else:
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_src,
                                                 attr_dst, 'A-TRANSFER', '')
                                    insert_event(tablename, maincallid, str(row_blindtr[2]), attr_dst,
                                                 attr_dst, 'ANSWER', '')
                    else:
                        if current_src == '':
                            insert_event(tablename, maincallid, time_q, row_q[2], row_q[3], row_q[4], '')
                        else:
                            insert_event(tablename, maincallid, time_q, row_q[2], current_src, row_q[4], '')
            else:
                if current_src == '':
                    insert_event(tablename, maincallid, time_q, row_q[2], row_q[3], row_q[4], '')
                else:
                    insert_event(tablename, maincallid, time_q, row_q[2], current_src, row_q[4], '')

                    # Значит это bridge-app ATXFER, и нам надо найти channel2_name, и по нему из cdr
                    # найти dst (Select dst from cdr Where channel = channel2_name)

                    # Тут мы подразумеваем, что все очереди и трансферы за их пределами закончились,
                    # и ловим последний hangup

        elif row_q[4] == 'RINGNOANSWER':
            # Если при звонке агенту очереди, он сбрасывает вызов, либо происходит ошибка соединения,
            # в логе он все-равно отображается как RINGNOANSWER.
            # Чтобы выделить такой вариант развития событий, если время ожидания на агенте составляет 90% или менее
            # таймаута для очереди, пишем событие BUSY.
            reason = row_q[4]
            cur_timeout = con.cursor()
            cur_timeout.execute(query31, [row_q[2]])
            if cur_timeout.rowcount != 0:
                res_timeout = cur_timeout.fetchone()
                timeout = int(res_timeout[0])
                if int(row_q[5])/(timeout*1000) <= 0.9:
                    reason = 'BUSY'
            if reason == 'BUSY' and is_ringall == 1:
                pass
            else:
                if current_src == '':
                    insert_event(tablename, maincallid, time_q, row_q[2], row_q[3], reason, '')
                else:
                    insert_event(tablename, maincallid, time_q, row_q[2], current_src, reason, '')

        else:
            if current_src == '':
                insert_event(tablename, maincallid, time_q, row_q[2], row_q[3], row_q[4], '')
            else:
                insert_event(tablename, maincallid, time_q, row_q[2], current_src, row_q[4], '')

    cur_q.close()


def check_transfer(dst):
    """Проверяем, куда трансфер. Если в очередь, то в result имя очереди, если нет - пустая строка"""
    cur_tr = con.cursor()
    cur_tr.execute(query9, [dst])
    if cur_tr.rowcount == 0:
        cur_tr.close()
        return ''
    else:
        result_tr = cur_tr.fetchone()
        qname = regexp6.match(result_tr[0])
        cur_tr.close()
        if qname:
            return qname.group(1)
        else:
            return 'SomeQueue'  # если не удалось выделить имя очереди регуляркой


def find_pickup_after_transfer(table_name, mainid, time_tr, src_num_tr, tr_type):
    """Проверяем, нет ли перехвата вызова после события TRANSFER из очереди, затем, в любом случае,
     проверяем на наличие дальнейших BLINDTRANSFER и PICKUP после них, т.к. звонок из очереди уже вышел"""

    if tr_type == 'ATXFER':
        pass
    else:
        cur_pickup = con.cursor()
        cur_pickup.execute(query71, [time_tr[0:19], src_num_tr, mainid])
        if cur_pickup.rowcount != 0:
            result_pickup = cur_pickup.fetchone()
            if result_pickup[0] == 'PICKUP':
                pickup_dict = eval(result_pickup[3])

                if_match = regexp_sip.match(pickup_dict['pickup_channel'])
                pickup_dst = ''
                if if_match:
                    pickup_dst = if_match.group(1)

                insert_event(table_name, mainid, str(result_pickup[1]), src_num_tr, pickup_dst, 'PICKUP', '')
            elif result_pickup[0] == 'ANSWER':

                insert_event(table_name, mainid, str(result_pickup[1]), src_num_tr, src_num_tr, 'ANSWER', '')
            elif result_pickup[0] == 'HANGUP':
                cur_fail = con.cursor()
                cur_fail.execute(query72, [result_pickup[2], mainid])
                if cur_fail.rowcount != 0:
                    result_fail = cur_fail.fetchone()
                    insert_event(table_name, mainid, str(result_pickup[1]), src_num_tr, src_num_tr, result_fail[0], '')
                cur_fail.close()
        cur_pickup.close()


def check_uniqueid(check_uid, check_lid):
    """Проверяем вхождение uniqueid в звонок, который в данный момент разбираем.
     Т.е., если cel.uniqueid=check_uid присутствует в звонке, где cel.linkedid=check_lid,
     делаем вывод, что часть звонка с данным uniqueid входит в разбираемый звонок"""

    cur_uid = con.cursor()
    cur_uid.execute(query5, [check_uid, check_lid])
    result_uid = cur_uid.fetchone()
    cur_uid.close()
    if result_uid[0] == 0:
        return 0  # Это отдельный звонок, либо часть другого звонка
    else:
        return 1  # Этот звонок входит в состав разбираемого звонка


def check_userfield(check_ufield):
    """ Проверяем уникальность userfield. Если такого нет в поле linkedid в таблице cel,
      значит этот id входит  в состав другого звонка"""
    cur_chk = con.cursor()
    cur_chk.execute(query21, [check_ufield])
    result_chk = cur_chk.fetchone()
    if result_chk[0] == 0:
        return 0, ''  # Этот звонок является частью другого
    else:
        cur_chk.execute(query22, [check_ufield])
        if cur_chk.rowcount == 0:
            cur_chk.close()
            return 1, ''  # Это отдельный звонок, вызов еще не завершен
        else:
            result_chk = cur_chk.fetchone()
            cur_chk.close()
            return 2, str(result_chk[0])  # Это отдельный звонок, вызов завершен


def main_in_routine(ufield_mr):
    """ Тут описание главной процедуры
    """
    global current_src
    global temp_table

    cur_mr = con.cursor()
    for i in range(len(ufield_mr)):
        check_ufield, linkedid_end_time = check_userfield(ufield_mr[i])
        if check_ufield == 2:  # Если это отдельный звонок, и вызов окончен
            cur_mr.execute(query11, [ufield_mr[i]])
            if cur_mr.rowcount != 0:
                result_mr = cur_mr.fetchone()
                time_mr = str(result_mr[0])
                src_mr = result_mr[2]
                dst_mr = result_mr[3]
                data_mr = ''
                if result_mr[4] != 'Queue':
                    cur_mr.execute(query12, [ufield_mr[i]])
                    if cur_mr.rowcount != 0:
                        result_mr = cur_mr.fetchone()
                        time_mr = str(result_mr[0])
                        src_mr = result_mr[2]
                        dst_mr = result_mr[3]
                # Смотрим, откуда (8-800... и т.д.) пришел звонок, для этого разбираем поле clid из таблицы cdr
                # Если обе части поля совпадают - значит это звонок на городскую линию, и в data_mr ничего не пишем
                # Иначе, пишем в data_mr источник вызова
                if_match = regexp2.match(result_mr[1])
                if if_match:
                    if if_match.group(1) != if_match.group(2) and if_match.group(2) == '88342270703':
                        data_mr = if_match.group(2)
                        src_mr = if_match.group(1)
                    elif if_match.group(1) != if_match.group(2):
                        data_mr = if_match.group(1)

                temp_table = []
                insert_event('z_queue_events', ufield_mr[i], time_mr, src_mr, dst_mr, 'START', data_mr)
                current_src = ''
                queue_log_read(ufield_mr[i], ufield_mr[i], 'z_queue_events')

                # сортируем
                temp_table.sort(key=lambda i: i[1])
                # дописываем запись с END и пишем в БД
                insert_event('z_queue_events', ufield_mr[i], linkedid_end_time, src_mr, dst_mr, 'END', '')
                rebuild_transfers()
                write_in_base('z_queue_events')

        elif check_ufield == 1:  # Если это отдельный звонок, но вызов не окончен
            insert_last(ufield_mr[i], 'INUSE_IN')

    cur_mr.close()


def main_out_routine(ufield_mr):
    """ Тут описание главной процедуры
    """
    global transfer_count
    global current_src
    global temp_table

    cur_mr = con.cursor()
    for i in range(len(ufield_mr)):
        check_ufield, linkedid_end_time = check_userfield(ufield_mr[i])
        if check_ufield == 2:  # Если это отдельный звонок, и вызов окончен
            cur_mr.execute(query14, [ufield_mr[i]])
            if cur_mr.rowcount != 0:
                result_mr = cur_mr.fetchone()
                src_mr = result_mr[3]
                if len(src_mr) > 4:
                    if_match = regexp2.match(result_mr[2])
                    if if_match:
                        src_mr = if_match.group(1)
                time_mr = str(result_mr[0])
                dst_mr = result_mr[4]
                data_mr = result_mr[6]

                temp_table = []
                insert_event('z_outcall_events', ufield_mr[i], time_mr, src_mr, dst_mr, 'START', data_mr)
                current_src = ''
                #######
                if data_mr == 'ANSWERED':
                    cur_blindtr = con.cursor()  # Выбираем все оставшиеся TRANSFER после выхода из очереди
                    cur_blindtr.execute(query8, [ufield_mr[i], time_mr])
                    if cur_blindtr.rowcount != 0:
                        result_blindtr = cur_blindtr.fetchall()
                        cur_subattr = con.cursor()
                        for row_blindtr in result_blindtr:
                            # Если трансфер в очередь, то пишем соответствующее событие и прерываем
                            # дальнейшую обработку трансферов
                            attr_src = row_blindtr[4]
                            if len(attr_src) > 4:
                                attr_src = row_blindtr[3]
                            if row_blindtr[1] == 'BLINDTRANSFER' and row_blindtr[0] not in transfer_count:
                                tr_dict = eval(row_blindtr[5])
                                transfer_dst = tr_dict['extension']
                                if check_transfer(transfer_dst) != '':
                                    transfer_count.append(row_blindtr[0])
                                    insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_src,
                                                 transfer_dst, 'B-TRANSFER', '')
                                    cur_queue = con.cursor()
                                    cur_queue.execute(query90, [ufield_mr[i], ufield_mr[i]])
                                    if cur_queue.rowcount != 0:
                                        res_queue = cur_queue.fetchall()
                                        for row_queue in res_queue:
                                            queue_log_read(row_queue[0], ufield_mr[i], 'z_outcall_events')
                                    cur_queue.close()
                                    break
                                if attr_src != '':
                                    transfer_count.append(row_blindtr[0])
                                    insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_src,
                                                 transfer_dst, 'B-TRANSFER', '')
                                    find_pickup_after_transfer('z_outcall_events', ufield_mr[i], str(row_blindtr[2]),
                                                               transfer_dst, 'BLINDXFER')
                            elif row_blindtr[1] == 'ATTENDEDTRANSFER' and row_blindtr[0] not in transfer_count:
                                tr_dict = eval(row_blindtr[5])
#                                attr_src = row_blindtr[3]
                                attr_dst = ''
                                transfer_count.append(row_blindtr[0])
                                if 'bridge2_id' in tr_dict:  # Если это bridge-bridge ATXFER
                                    tr_bridge2_id = tr_dict['bridge2_id']
                                    cur_subattr.execute(query82, [ufield_mr[i]])
                                    if cur_subattr.rowcount != 0:
                                        result_subattr = cur_subattr.fetchall()
                                        for subattr in result_subattr:
                                            attr_dict = eval(subattr[1])
                                            if attr_dict['bridge_id'] == tr_bridge2_id:
                                                attr_dst = subattr[0]
                                    if check_transfer(attr_dst) != '':
                                        insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        cur_queue = con.cursor()
                                        cur_queue.execute(query90, [ufield_mr[i], ufield_mr[i]])
                                        if cur_queue.rowcount != 0:
                                            res_queue = cur_queue.fetchall()
                                            for row_queue in res_queue:
                                                queue_log_read(row_queue[0], ufield_mr[i], 'z_outcall_events')
                                        cur_queue.close()
                                        break
                                    else:
                                        insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_dst,
                                                     attr_dst, 'ANSWER', '')
                                elif 'app' in tr_dict:  # Если это bridge-app ATXFER
                                    cur_subattr.execute(query84, [tr_dict['channel2_name'],
                                                                  tr_dict['channel2_uniqueid']])
                                    if cur_subattr.rowcount != 0:
                                        result_subattr = cur_subattr.fetchone()
                                        attr_dst = result_subattr[0]
                                    if check_transfer(attr_dst) != '':
                                        insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        cur_queue = con.cursor()
                                        cur_queue.execute(query90, [ufield_mr[i], ufield_mr[i]])
                                        if cur_queue.rowcount != 0:
                                            res_queue = cur_queue.fetchall()
                                            for row_queue in res_queue:
                                                queue_log_read(row_queue[0], ufield_mr[i], 'z_outcall_events')
                                        cur_queue.close()
                                        break
                                    else:
                                        insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_src,
                                                     attr_dst, 'A-TRANSFER', '')
                                        insert_event('z_outcall_events', ufield_mr[i], str(row_blindtr[2]), attr_dst,
                                                     attr_dst, 'ANSWER', '')

                                    # Значит это bridge-app ATXFER, и нам надо найти channel2_name, и по нему из cdr
                                    # найти dst (Select dst from cdr Where channel = channel2_name)
                                    # Тут мы подразумеваем, что все очереди и трансферы за их пределами закончились,
                                    # и ловим последний hangup
                    else:
                        pass
                    cur_blindtr.close()
                # TODO
                # Тут подразумеваем, что осталось записать только END, но в случае, если последнее событие последней
                # очереди было EXITWITHTIMEOUT, то возможно дальше следует продолжение вызова с переходами по диалплану
                # без очередей
                # Делаем это после сортировки, НО перед добавлением события END
                # 1. Проверяем, если последнее событие - EXITWITHTIMEOUT

                #######
                # сортируем
                temp_table.sort(key=lambda i: i[1])
                # дописываем запись с END и пишем в БД
                sysagr_var = ''
                cur_sysagr = con.cursor()
                cur_sysagr.execute("SELECT * FROM sysagr_vars WHERE linkedid = %s", [ufield_mr[i]])
                if cur_sysagr.rowcount != 0:
                    res_sysagr = cur_sysagr.fetchone()
                    sysagr_var = res_sysagr[2]

                insert_event('z_outcall_events', ufield_mr[i], linkedid_end_time, src_mr, dst_mr, 'END', sysagr_var)
                rebuild_transfers()
                write_in_base('z_outcall_events')

        elif check_ufield == 1:  # Если это отдельный звонок, но вызов не окончен
            insert_last(ufield_mr[i], 'INUSE_OUT')

    cur_mr.close()


def insert_last(l_id, l_status):
    """Вставляем записи в нашу таблицу статусов"""

    cur_ins = con.cursor()
    cur_ins.execute(query111, [l_id, l_status])
    cur_ins.close()


def insert_event(e_table, l_id, e_time, e_src, e_dst, e_event, e_data):
    """Вставляем записи событий в нашу таблицу"""
    global temp_table

    e_src_n = check_number(e_src)  # Проверяем src и dst, чтобы вместо имен
    e_dst_n = check_number(e_dst)  # очередей везде были их номера

    temp_table.append((l_id, e_time, e_src_n, e_dst_n, e_event, e_data))


def rebuild_transfers():
    """Вставляем на место трансферы"""
    global temp_table
    transfer_pos = []  # индексы строк с событием A-Transfer
    i = 0

    for temp_row in temp_table:
        if temp_row[4] == 'A-TRANSFER':
            transfer_pos.append(i)
        i += 1

    for i in transfer_pos:
        j = i-1
        dst = temp_table[i][3]

        while not j < 1:
            temp_row = temp_table[j]
            if temp_row[2] == dst and temp_row[4] in ('ENTERQUEUE',):
                break
            j -= 1

        if j > 0:
            current_transfer = temp_table.pop(i)
            temp_table.insert(j, current_transfer)


def write_in_base(w_table):
    """Записываем данные в таблицу"""
    global temp_table

    cur_ins = con.cursor()
    if w_table == 'z_queue_events':
        cur_ins.executemany(query110, temp_table)
    elif w_table == 'z_outcall_events':
        cur_ins.executemany(query109, temp_table)
    cur_ins.close()


def check_number(chk_num):
    """ Проверяем, чтобы во всех src и dst вместо имен очередей, были их номера"""
    if chk_num == 'NONE':
        return chk_num
    elif regexp1.search(chk_num):
        chk_num_s = 'QueueRoutine,' + chk_num + '%'
        cur_check = con.cursor()
        cur_check.execute(query101, [chk_num_s])
        if cur_check.rowcount != 0:
            result_check = cur_check.fetchone()
            return result_check[0]
        else:
            return chk_num
    else:
        return chk_num


def clear_last_table(l_status):
    """Очищаем таблицу статусов"""

    cur_ins = con.cursor()
    cur_ins.execute('DELETE FROM z_queue_last WHERE status = %s', l_status)
    cur_ins.close()
    con.commit()


# Собственно, тело самой программы :)
logwrite(logpath, 'Start script')

today = str(datetime.date.today())  # today date
# today = '2018-08-27'

try:
    con = MySQLdb.connect(dbhost, dbuser, dbpwd, dbname)
    cur = con.cursor()
    cur.execute(encode_query)

    logwrite(logpath, 'Incoming calls')

    cur.execute(query03, [today + '%'])  # Select "linkedid" all processed Incoming calls for today
    if cur.rowcount != 0:
        result = cur.fetchall()
        for row in result:
            avail_in.append(str(row[0]))

    cur.execute(query00, ['INUSE_IN'])  # Select in "ufield" calls with status INUSE_IN с прошлой проверки
    if cur.rowcount != 0:
        result = cur.fetchall()
        clear_last_table('INUSE_IN')
        for row in result:
            if str(row[0]) not in avail_in:
                ufield_in.append(str(row[0]))

    cur.execute(query00, ['LAST_IN'])  # Select in "last_id" содержимое "linkedid" last processed Incoming call
    if cur.rowcount != 0:
        result = cur.fetchall()
        clear_last_table('LAST_IN')
        for row in result:
            last_id = row[0]

    cur.execute(query02, [today + '%'])  # Select in ufield new Incoming calls (add to selected with status INUSE_IN)
    if cur.rowcount != 0:
        result = cur.fetchall()
        for row in result:
            if (str(row[0]) not in avail_in) and (str(row[0]) not in ufield_in):
                ufield_in.append(str(row[0]))
    if len(ufield_in) != 0:
        main_in_routine(ufield_in)
        insert_last(ufield_in[len(ufield_in) - 1], 'LAST_IN')
    elif last_id != '':
        insert_last(last_id, 'LAST_IN')

    logwrite(logpath, 'Outgoing calls')

    cur.execute(query04, [today + '%'])  # Select "linkedid" all processed Outgoing calls for today
    if cur.rowcount != 0:
        result = cur.fetchall()
        for row in result:
            avail_out.append(str(row[0]))

    cur.execute(query00, ['INUSE_OUT'])  # Выбираем в "ufield_out" вызовы со статусом INUSE_OUT с прошлой проверки
    if cur.rowcount != 0:
        result = cur.fetchall()
        clear_last_table('INUSE_OUT')
        for row in result:
            if str(row[0]) not in avail_out:
                ufield_out.append(str(row[0]))

    cur.execute(query00, ['LAST_OUT'])  # Select in "last_id" содержимое "linkedid" последнего обработанного Outgoing call
    if cur.rowcount != 0:
        result = cur.fetchall()
        clear_last_table('LAST_OUT')
        for row in result:
            last_id = row[0]

    cur.execute(query01, [today + '%'])  # выбираем в "ufield_out" новые исходящие (добавляем к выбранным с INUSE_OUT)
    if cur.rowcount != 0:
        result = cur.fetchall()
        for row in result:
            m = regexp4.match(str(row[0]))
            if m:
                if (m.group(1) not in avail_out) and (m.group(1) not in ufield_out):
                    ufield_out.append(m.group(1))
    if len(ufield_out) != 0:
        main_out_routine(ufield_out)
        insert_last(ufield_out[len(ufield_out) - 1], 'LAST_OUT')
    elif last_id != '':
        insert_last(last_id, 'LAST_OUT')

    cur.close()
    con.commit()
    con.close()

except MySQLdb.Error:
    logwrite(logpath, '-=== MySQLdb Error ===-')
    for err in sys.exc_info():
        logwrite(logpath, str(err))
    logwrite(logpath, '-=== MySQLdb Error ===-')
except:
    logwrite(logpath, '-=== Other Error ===-')
    for err in sys.exc_info():
        logwrite(logpath, str(err))
    logwrite(logpath, '-=== Other Error ===-')

logwrite(logpath, 'End script')
