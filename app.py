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

# # Parámetros de conexión a la base de datos
# db_params = {
#     "dbname": "concursos_presupuesto",
#     "user": "postgres",
#     "password": "123",
#     "host": "localhost"
# }

# Parámetros de conexión a la base de datos
db_params = {
    "dbname": "concursos_presupuesto",
    "user": "redciudadana",
    "password": "Jy3qGPRxNOb99ctNxzfK041tFm9V9d4O",
    "host": "dpg-cncd9bect0pc73fqrqu0-a.oregon-postgres.render.com",
    "port": "5432" # Add this if your database is using a non-default port
}

def get_db_connection():
    conn = psycopg2.connect(**db_params)
    return conn

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

def fetch_red_flags_red01():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    SELECT
        "nogConcurso",
        "descripcion",
        "entidadCompradora",
        "fechaDePublicacion",
        "fechaCierreRecepcionOfertas",
        "fechaCierreRecepcionOfertas" - "fechaDePublicacion" AS duracion_concurso_dias
    FROM concurso_presupuesto_2023
    WHERE "fechaCierreRecepcionOfertas" - "fechaDePublicacion" < 2
    ORDER BY duracion_concurso_dias ASC;
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

def fetch_red_flags_red02():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    SELECT
        "nogConcurso",
        "descripcion",
        "entidadCompradora",
        "fechaDePublicacion",
        "fechaCierreRecepcionOfertas",
        "fechaCierreRecepcionOfertas" - "fechaDePublicacion" AS duracion_concurso_dias
    FROM concurso_presupuesto_2023
    WHERE "fechaCierreRecepcionOfertas" - "fechaDePublicacion" > 120
    ORDER BY duracion_concurso_dias ASC;
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

def fetch_red_flags_red03():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    SELECT
        "nogConcurso",
        "descripcion",
        "entidadCompradora",
        "fechaDeUltimaAdjudicacion",
        "fechaDeAdjudicacion",
        "fechaDeUltimaAdjudicacion" - "fechaDeAdjudicacion" AS dias_de_retraso
    FROM concurso_presupuesto_2023
    WHERE
        "fechaDeUltimaAdjudicacion" - "fechaDeAdjudicacion" > 30 -- Asumiendo un umbral de 30 días como significativo
        AND "estatusDelConcurso" = 'Adjudicado' -- Filtro por concursos ya adjudicados
    ORDER BY dias_de_retraso DESC; -- Ordena los resultados para mostrar primero los de mayor retraso
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

def fetch_red_flags_red04():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    SELECT
        nombre,
        COUNT(*) AS cantidad_de_veces
    FROM concurso_presupuesto_2023
    GROUP BY nombre
    HAVING COUNT(*) <= 2
    ORDER BY cantidad_de_veces ASC, nombre;
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

def fetch_red_flags_red05():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)  
    cur.execute("""
    WITH promedio_categoria AS (
        SELECT
            "categorias",
            AVG("monto") AS promedio_monto
        FROM concurso_presupuesto_2023
        WHERE "estatusDelConcurso" = 'Adjudicado'
        GROUP BY "categorias"
    )
    SELECT
        cp."nogConcurso",
        cp."descripcion",
        cp."categorias",
        cp."monto",
        pc."promedio_monto",
        ROUND(ABS(cp."monto" - pc."promedio_monto") / pc."promedio_monto" * 100,2) AS desviacion_porcentual
    FROM concurso_presupuesto_2023 cp
    JOIN promedio_categoria pc ON cp."categorias" = pc."categorias"
    WHERE cp."estatusDelConcurso" = 'Adjudicado'
    AND ABS(cp."monto" - pc."promedio_monto") / pc."promedio_monto" * 100 > 100000 -- Ejemplo de umbral de desviación: 100000%
    ORDER BY desviacion_porcentual DESC;
    """)
    red_flags = cur.fetchall()
    cur.close()
    conn.close()
    return red_flags

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/redflags')
def redflags():
    return render_template('redflags.html')

@app.route('/red_flags/r024')
def red_flag_r024_page():
    red_flags = fetch_red_flags_r024()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_r024.html', red_flags=red_flags)

@app.route('/red_flags/red01')
def red_flag_red01_page():
    red_flags = fetch_red_flags_red01()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_red01.html', red_flags=red_flags)

@app.route('/red_flags/red02')
def red_flag_red02_page():
    red_flags = fetch_red_flags_red02()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_red02.html', red_flags=red_flags)

@app.route('/red_flags/red03')
def red_flag_red03_page():
    red_flags = fetch_red_flags_red03()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_red03.html', red_flags=red_flags)

@app.route('/red_flags/red04')
def red_flag_red04_page():
    red_flags = fetch_red_flags_red04()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_red04.html', red_flags=red_flags)

@app.route('/red_flags/red05')
def red_flag_red05_page():
    red_flags = fetch_red_flags_red05()
    # Renderiza una plantilla específica para esta red flag
    return render_template('red_flag_red05.html', red_flags=red_flags)

if __name__ == '__main__':
    app.run(debug=True)