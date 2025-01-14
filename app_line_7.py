import mysql.connector
import time
import schedule
import threading
import logging
from datetime import datetime, timedelta
from opc_reader import OPCReader
from utils import get_poste, calculer_kpi

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# Define OPC server URL and MySQL connection details
opc_url = "opc.tcp://192.168.2.140:4840"
mysql_config = {
    "host": "localhost",
    "database": "",
    "user": "root",
    "password": "password",
    "port": None,
}

# Create the OPCReader instance
opc_reader = OPCReader(opc_url)

job_kpi_lock = threading.Lock()

start_time = None
etat = True


# Attempt to connect to OPC and MySQL with error handling
def connect_to_opc():
    try:
        if opc_reader is None:
            raise ValueError("OPCReader instance is null")
        opc_reader.connect()
        logger.info("Connected to OPC server successfully.")
        return True
    except AttributeError as e:
        logger.error(f"OPCReader instance is not properly initialized: {e}")
    except Exception as e:
        logger.error(f"Error connecting to OPC server: {e}")
    return False


def connect_to_mysql():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        logger.info("Connection to MySQL database established.")
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to MySQL: {err}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if conn is None or cursor is None:
            logger.error("Failed to establish a MySQL connection.")
    return conn, cursor


while True:
    # Try connecting to OPC server
    if connect_to_opc():
        break
    else:
        logger.info("Retrying OPC server connection in 5 seconds...")
        time.sleep(5)

while True:
    # Try connecting to MySQL
    conn, cursor = connect_to_mysql()
    if conn and cursor:
        break
    else:
        logger.info("Retrying MySQL connection in 5 seconds...")
        time.sleep(5)


def lire_variables_sql():
    try:
        query = "SELECT * FROM OrdreFabrication WHERE Of_Prod = 1 ORDER BY Id LIMIT 1"
        cursor.execute(query)
        of_row = cursor.fetchone()

        query = "SELECT SUM(Quantite) FROM NonConforme WHERE Of = %s"
        cursor.execute(query, (of_row["Numero"],))
        total_nc_quantity = cursor.fetchone()[0]

        return {"of": of_row, "total_nc_quantity": total_nc_quantity}
    except mysql.connector.Error as err:
        logger.error(f"Error fetching SQL variables: {err}")


def handle_history(kpis, variables_opc):
    try:
        query = "INSERT INTO HistoriqueTemp (Poste, `Of`, TP, TQ, TD, QP, Debit) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        data = (
            get_poste(),
            lire_variables_sql()["of"]["Numero"],
            kpis["TP"],
            kpis["TQ"],
            kpis["TD"],
            variables_opc["conso"],
            variables_opc["debit"],
        )
        cursor.execute(query, data)
        conn.commit()
        logger.info(f"KPI recorded at {datetime.now()}")

    except mysql.connector.Error as err:
        logger.error(f"Error inserting KPI into database: {err}")


def handle_arret(variables_opc):
    global start_time, etat
    try:
        query = "INSERT INTO Arret (Poste, `Of`, Duree) VALUES (%s, %s, %s)"

        if variables_opc["etat_arret"] == 1 and etat:
            start_time = datetime.now()
            etat = False
        elif variables_opc["etat_arret"] == 0:
            etat = True
            duration = datetime.now() - start_time
            if duration > timedelta(minutes=5):
                data = (
                    get_poste(),
                    lire_variables_sql()["of"]["Numero"],
                    duration.total_seconds(),
                )
                cursor.execute(query, data)
                conn.commit()

    except mysql.connector.Error as err:
        logger.error(f"Error inserting Arret into database: {err}")


def job_kpi():
    with job_kpi_lock:
        try:
            variables_opc = opc_reader.lire_variables_opc()
            if variables_opc is None:
                logger.info("Variables OPC are None")
                return

            variables_sql = lire_variables_sql()
            if variables_sql is None:
                logger.info("Variables SQL are None")
                return

            kpi_values = calculer_kpi(variables_opc, variables_sql)

            if any(arg is None for arg in kpi_values.values()):
                logger.info("One or more KPI values are None")
                return

            handle_history(kpi_values, variables_opc)
            handle_arret(variables_opc)

        except Exception as e:
            logger.error(f"Error in job_kpi: {e}")


def process_data():
    try:
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=2)

        logger.info(f"Running process_data at {now}")
        logger.info(f"Fetching rows from {one_hour_ago} to {now}")

        cursor.execute(
            """
            SELECT TP, TQ, TD, QP, Debit
            FROM HistoriqueTemp
            WHERE Date >= %s
            ORDER BY Date DESC 
        """,
            (one_hour_ago,),
        )
        rows = cursor.fetchall()

        if not rows:
            logger.info("No data found for processing.")
            return

        avg_tp = sum(row[0] for row in rows) / len(rows)
        avg_tq = sum(row[1] for row in rows) / len(rows)
        avg_td = sum(row[2] for row in rows) / len(rows)
        avg_qp = rows[0][4]
        avg_debit = sum(row[5] for row in rows) / len(rows)

        poste = get_poste()
        of = lire_variables_sql()["of"]["Numero"]

        try:
            cursor.execute(
                """
                INSERT INTO Historique (Date, Poste, `Of`, TP, TQ, TD, QP, Debit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    now.replace(minute=0, second=0, microsecond=0),
                    poste,
                    of,
                    avg_tp,
                    avg_tq,
                    avg_td,
                    avg_qp,
                    avg_debit,
                ),
            )

            cursor.execute(
                """
                DELETE FROM HistoriqueTemp
            """
            )

            conn.commit()
            logger.info(f"Data processed and inserted successfully at {now}")
        except mysql.connector.Error as err:
            logger.error(f"Error inserting averages into database: {err}")
        except Exception as e:
            logger.error(f"Unexpected error during data processing: {e}")
    except Exception as e:
        logger.error(f"Error in process_data: {e}")


def delete_duplicates():
    try:
        query = """
            DELETE t1 FROM Historique t1
            INNER JOIN Historique t2 
            WHERE 
                t1.id < t2.id AND 
                t1.Date = t2.Date
        """
        cursor.execute(query)
        conn.commit()
        logger.info("Duplicates deleted from Historique table.")
    except mysql.connector.Error as err:
        logger.error(f"Error deleting duplicates from Historique table: {err}")
    except Exception as e:
        logger.error(f"Unexpected error during deleting duplicates: {e}")


def main():
    schedule.every(30).seconds.do(job_kpi)
    schedule.every().hour.at(":00").do(process_data)
    schedule.every().hour.at(":00").do(delete_duplicates)

    logger.info("Starting monitoring system...")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as err:
            logger.error(f"Error in main loop: {err}")
            time.sleep(5)  # Wait before retrying


if __name__ == "__main__":
    main()
