from flask import Flask, render_template, jsonify
import psycopg2
import json
from decimal import Decimal
from psycopg2.extras import RealDictCursor

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert Decimal to string to maintain precision
            return str(obj)
        # Para otros tipos, usa el método de serialización por defecto.
        return super(CustomJSONEncoder, self).default(obj)

app = Flask(__name__)

# Configura el codificador personalizado para la aplicación Flask.
app.json_encoder = CustomJSONEncoder

# Parámetros de conexión a la base de datos
db_params = {
    "dbname": "concursos_presupuesto",
    "user": "postgres",
    "password": "123",
    "host": "localhost"
}

def get_db_connection():
    conn = psycopg2.connect(**db_params)
    return conn

def fetch_red_flags_r028():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    SELECT "nogConcurso", COUNT(*) as count
    FROM concurso_presupuesto_2023
    GROUP BY "nogConcurso", "monto"
    HAVING COUNT(*) > 1
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

def fetch_red_flags_r024():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    SELECT "nogConcurso", COUNT(*)
    FROM concurso_presupuesto_2023
    GROUP BY "nogConcurso", "monto"
    HAVING COUNT(*) > 1;

    WITH ofertas_ordenadas AS (
        SELECT
            "nogConcurso",
            "nombre",
            "monto",
            RANK() OVER (PARTITION BY "nogConcurso" ORDER BY "monto" ASC) AS ranking
        FROM concurso_presupuesto_2023
        WHERE "estatusDelConcurso" = 'Adjudicado' and "monto" > 0 -- Asumiendo que solo nos interesan los concursos adjudicados
    ),
    comparacion_ofertas AS (
        SELECT
            a."nogConcurso",
            a."nombre" AS oferta_ganadora,
            a."monto" AS monto_ganador,
            b."nombre" AS segunda_oferta_mas_baja,
            b."monto" AS monto_segundo,
            (b."monto" - a."monto") / a."monto" * 100 AS diferencia_porcentual
        FROM ofertas_ordenadas a
        JOIN ofertas_ordenadas b ON a."nogConcurso" = b."nogConcurso" AND b.ranking = a.ranking + 1
        WHERE a.ranking = 1
    )
    SELECT *
    FROM comparacion_ofertas
    WHERE diferencia_porcentual < 5; -- Umbral de diferencia porcentual atípicamente baja
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/red_flags/r024')
def red_flag_r024_page():
    red_flags = fetch_red_flags_r024()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_r024.html', red_flags=red_flags)

@app.route('/red_flags/r028')
def red_flag_r028_page():
    red_flags = fetch_red_flags_r028()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_r028.html', red_flags=red_flags)

if __name__ == '__main__':
    app.run(debug=True)