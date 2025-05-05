import mysql.connector
from datetime import datetime, timedelta

# === 1. Połączenia ===
sakila_conn = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='root',
    password='#########',
    database='sakila'
)

mindsdb_conn = mysql.connector.connect(
    host='127.0.0.1',
    port=47335,
    user='mindsdb',
    password='',
    database='mindsdb'
)

# === 2. Funkcja do sprawdzenia dostępności filmu ===
def check_film_availability(store_id, film_id, current_datetime):
    sakila_cursor = sakila_conn.cursor()

    # Sprawdzenie, czy film istnieje w inwentarzu sklepu
    inventory_query = """
    SELECT i.inventory_id, i.film_id
    FROM inventory i
    WHERE i.film_id = %s AND i.store_id = %s;
    """
    sakila_cursor.execute(inventory_query, (film_id, store_id))
    inventory_data = sakila_cursor.fetchall()
    if not inventory_data:
        print(f"Film o ID {film_id} nie znajduje się w inwentarzu sklepu {store_id}.")
        return

    inventory_ids = [str(record[0]) for record in inventory_data]
    placeholders = ', '.join(['%s'] * len(inventory_ids))

    # Sprawdzenie wypożyczeń (bez zwrotu)
    rental_query = f"""
    SELECT r.inventory_id, r.rental_date, r.customer_id
    FROM rental r
    WHERE r.inventory_id IN ({placeholders})
      AND (r.return_date IS NULL OR r.return_date > %s)
      AND r.rental_date <= %s;
    """
    params = inventory_ids + [current_datetime, current_datetime]
    sakila_cursor.execute(rental_query, params)
    rental_data = sakila_cursor.fetchall()

    rented_inventory = [(r[0], r[1], r[2]) for r in rental_data]
    available_inventory = [inventory_id for inventory_id, _ in inventory_data if inventory_id not in [r[0] for r in rented_inventory]]

    if available_inventory:
        print(f"Film o ID {film_id} jest dostępny w sklepie {store_id}.")
    else:
        print(f"Wszystkie kopie filmu o ID {film_id} są wypożyczone w sklepie {store_id}.")

        # Pobranie danych filmu (raz, bo są stałe)
        film_query = """
        SELECT rental_duration, length, replacement_cost
        FROM film
        WHERE film_id = %s;
        """
        sakila_cursor.execute(film_query, (film_id,))
        film_info = sakila_cursor.fetchone()
        if not film_info:
            print(f"Nie znaleziono danych filmu o ID {film_id}.")
            return
        rental_duration, length, replacement_cost = film_info

        # Przewidywanie daty zwrotu dla każdej kopii
        for inventory_id, rental_date, customer_id in rented_inventory:
            rental_weekday = rental_date.weekday() + 1 

            # Predykcja w MindsDB
            prediction_query = """
            SELECT actual_rental_duration_seconds
            FROM return_dur13_predictor
            WHERE rental_duration = %s
              AND length = %s
              AND replacement_cost = %s
              AND rental_day_of_week = %s
              AND customer_id = %s;
            """
            mindsdb_cursor = mindsdb_conn.cursor()
            mindsdb_cursor.execute(prediction_query, (
                rental_duration, length, replacement_cost, rental_weekday, customer_id
            ))
            prediction_data = mindsdb_cursor.fetchone()

            if not prediction_data:
                print(f"Brak predykcji dla filmu ID {film_id}, klienta {customer_id}.")
                continue

            predicted_duration_seconds = prediction_data[0]
            predicted_return_date = rental_date + timedelta(seconds=predicted_duration_seconds)
            print(f"Film o ID {film_id} (inventory_id: {inventory_id}) zostanie prawdopodobnie zwrócony około {predicted_return_date.strftime('%Y-%m-%d %H:%M:%S')}.")

# === 3. Testowanie ===
store_id = int(input("Podaj store_id: "))
film_id = int(input("Podaj film_id: "))
current_datetime = datetime.strptime('2005-08-25 16:00:00', '%Y-%m-%d %H:%M:%S')

check_film_availability(store_id, film_id, current_datetime)

# === 4. Zamknięcie połączeń ===
sakila_conn.close()
mindsdb_conn.close()
