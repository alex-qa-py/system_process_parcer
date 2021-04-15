import sqlite3
import subprocess as sub
from datetime import datetime
from sqlite3 import Error

ARGS = ["ps", "aux", "--no-headers"]
CREATE_TABLE_RESULTS = "CREATE TABLE IF NOT EXISTS RESULTS ([USER] text, [PID] integer, [CPU] real, [MEM] real, " \
                       "[COMMAND] text) "
INSERT_INTO_RESULTS = "INSERT INTO RESULTS (CPU,MEM,PID,USER, COMMAND) VALUES (?,?,?,?,?)"
USERS_SQL = "SELECT DISTINCT USER FROM RESULTS"
PROCESS_COUNT_SQL = "SELECT COUNT(DISTINCT PID) FROM RESULTS"
USERS_PROCESS = "SELECT USER, COUNT(DISTINCT PID) FROM RESULTS GROUP BY USER"
MEMORY_USAGE_SQL = "SELECT SUM(MEM) FROM RESULTS"
CPU_USAGE_SQL = "SELECT SUM(CPU) FROM RESULTS"
TOP_PROCESS_USAGE = "SELECT COMMAND, max(CPU) FROM RESULTS"
TOP_MEMORY_USAGE = "SELECT COMMAND, max(MEM) FROM RESULTS"
DELETE_RESULTS = "DELETE FROM RESULTS"


def create_connection():
    conn = None
    try:
        conn = sqlite3.connect("ps.db")
    except Error as e:
        print(e.args[0])
    return conn


def insert_stdout_ps(conn):
    with conn:
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_RESULTS)
        result = sub.Popen(ARGS, stdout=sub.PIPE)
        for line in result.stdout:
            res = line.decode("UTF-8").split()
            cursor.execute(INSERT_INTO_RESULTS, (float(res[2]), float(res[3]), int(res[1]), res[0], res[10]))
        conn.commit()


def create_log_file(conn):
    with conn:
        cursor = conn.cursor()
        users = cursor.execute(USERS_SQL).fetchall()
        process_count = cursor.execute(PROCESS_COUNT_SQL).fetchall()
        users_process = cursor.execute(USERS_PROCESS).fetchall()
        memory_usage = cursor.execute(MEMORY_USAGE_SQL).fetchall()
        cpu_usage = cursor.execute(CPU_USAGE_SQL).fetchall()
        top_process_cpu_usage = cursor.execute(TOP_PROCESS_USAGE).fetchall()
        top_process_mem_usage = cursor.execute(TOP_MEMORY_USAGE).fetchall()

        data = f"Пользователи системы: {users} \n" \
               f"Процессов запущено: {process_count} \n" \
               f"Процессы по пользователям: {users_process} \n" \
               f"Всего памяти используется: {memory_usage} \n" \
               f"Всего CPU используется: {cpu_usage} \n" \
               f"Больше всего памяти ест: {top_process_mem_usage} \n" \
               f"Больше всего CPU ест: {top_process_cpu_usage}"

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H:%M")
        with open(f"{dt_string}-scan.txt", "w") as wr:
            wr.write(data)


def clear_results_table(conn):
    with conn:
        cursor = conn.cursor()
        cursor.execute(DELETE_RESULTS)
        conn.commit()


if __name__ == "__main__":
    connection = create_connection()
    insert_stdout_ps(connection)
    create_log_file(connection)
    clear_results_table(connection)
