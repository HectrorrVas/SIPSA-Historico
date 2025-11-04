from flask import Flask, render_template, request
import pandas as pd
import numpy as np
import os, re, unicodedata
import plotly.graph_objects as go
import plotly.express as px
from statsmodels.nonparametric.smoothers_lowess import lowess
from tqdm import tqdm
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# =========================
# ⚙️ Configuración
# =========================
BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datos", "SIPSA_Historico")

opciones_hoja = {
    "1.1": "Verduras y hortalizas",
    "1.2": "Frutas frescas",
    "1.3": "Tubérculos, raíces y plátanos",
    "1.4": "Granos y cereales",
    "1.5": "Huevos y lácteos",
    "1.6": "Carnes",
    "1.7": "Pescados",
    "1.8": "Productos procesados",
    "1.9": "Abastecimiento semanal por grupo de alimentos"
}


# =========================
# 🧩 Funciones auxiliares CORREGIDAS
# =========================
def normalizar(texto):
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize('NFD', texto)
    texto = ''.join([c for c in texto if unicodedata.category(c) != 'Mn'])
    return texto.lower().strip()


def extraer_fecha(nombre_archivo):
    """Función corregida para extraer fechas de archivos SIPSA"""
    nombre = os.path.basename(nombre_archivo).lower()

    # Mapeo de meses
    meses = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }

    # Patrones para diferentes formatos de archivo SIPSA
    patrones = [
        # Formato: anex-SIPSASemanal-02ago08ago-2025.xlsx
        r'(\d{1,2})([a-z]{3})(\d{1,2})([a-z]{3})-(\d{4})',
        # Formato: anex-SIPSASemanal-05abr-11abr-2025.xlsx
        r'(\d{1,2})([a-z]{3})-(\d{1,2})([a-z]{3})-(\d{4})',
        # Formato: anex-SIPSASemanal-11ene-17ene-2025.xlsx
        r'(\d{1,2})([a-z]{3})-(\d{1,2})([a-z]{3})-(\d{4})',
        # Formato simple con solo año
        r'(\d{4})'
    ]

    for patron in patrones:
        m = re.search(patron, nombre)
        if m:
            if len(m.groups()) == 5:
                # Formato con dos fechas: 02ago08ago-2025
                dia1, mes1, dia2, mes2, anio = m.groups()
                mes_num = meses.get(mes2.lower(), '01')
                fecha = f"{anio}-{mes_num}-{dia2.zfill(2)}"
                logger.info(f"Fecha extraída: {fecha} de {nombre}")
                return fecha
            elif len(m.groups()) == 1:
                # Solo año encontrado
                anio = m.group(1)
                fecha = f"{anio}-01-01"
                logger.info(f"Solo año encontrado: {fecha} de {nombre}")
                return fecha

    logger.warning(f"No se pudo extraer fecha del archivo: {nombre}")
    return None


def procesar_boletin(path, hoja, producto_objetivo, ciudad_objetivo):
    df = None
    for header_row in range(5, 15):
        try:
            temp = pd.read_excel(path, sheet_name=hoja, header=header_row)
            columnas_lower = [str(c).lower() for c in temp.columns]
            if "producto" in columnas_lower:
                df = temp
                logger.info(f"Header encontrado en fila {header_row} para {os.path.basename(path)}")
                break
        except Exception:
            continue

    if df is None:
        logger.warning(f"No se pudo leer el archivo {path}")
        return pd.DataFrame()

    df.columns = df.columns.str.strip().str.lower()

    # VERIFICAR COLUMNAS EXISTENTES - DEBUG
    logger.info(f"Columnas encontradas en {os.path.basename(path)}: {df.columns.tolist()}")

    if "producto" not in df.columns or "mercado mayorista" not in df.columns:
        logger.warning(f"Columnas necesarias no encontradas en {path}")
        return pd.DataFrame()

    df["producto_norm"] = df["producto"].apply(normalizar)
    df["mercado_norm"] = df["mercado mayorista"].apply(normalizar)

    producto_buscado = normalizar(producto_objetivo)
    ciudad_buscada = normalizar(ciudad_objetivo)

    mask_prod = df["producto_norm"].str.contains(producto_buscado, na=False)
    mask_ciud = df["mercado_norm"].str.contains(ciudad_buscada, na=False)

    df_filtrado = df[mask_prod & mask_ciud].copy()

    if df_filtrado.empty:
        logger.info(f"No se encontró '{producto_objetivo}' en '{ciudad_objetivo}' en {os.path.basename(path)}")
        return pd.DataFrame()

    # RENOMBRAR COLUMNAS - VERSIÓN ROBUSTA
    # Crear nuevo DataFrame con las columnas que necesitamos
    datos_procesados = []

    for _, fila in df_filtrado.iterrows():
        registro = {
            'producto': fila['producto'],
            'mercado': fila['mercado mayorista'],  # Usar el nombre original
            'archivo': os.path.basename(path),
            'boletin': extraer_fecha(path)
        }

        # Buscar precios en las columnas disponibles
        if 'pesos por kilogramo' in df_filtrado.columns:
            registro['precio_minimo'] = fila['pesos por kilogramo']

        # Buscar columnas unnamed para precios máximo y medio
        for idx, col in enumerate(df_filtrado.columns):
            if 'unnamed' in str(col).lower():
                if idx == 3:  # Columna 3 es precio_maximo
                    registro['precio_maximo'] = fila[col]
                elif idx == 4:  # Columna 4 es precio_medio
                    registro['precio_medio'] = fila[col]

        datos_procesados.append(registro)

    df_final = pd.DataFrame(datos_procesados)
    logger.info(f"Procesados {len(df_final)} registros con columnas: {df_final.columns.tolist()}")

    return df_final


# =========================
# 🌐 Rutas Flask
# =========================
@app.route("/")
def index():
    return render_template("index.html", opciones_hoja=opciones_hoja)


@app.route("/analizar", methods=["POST"])
def analizar():
    try:
        anio_objetivo = request.form["anio"]
        hoja = request.form["hoja"]
        producto_objetivo = request.form["producto"]
        ciudad_objetivo = request.form["ciudad"]

        carpeta_base = os.path.join(BASE_PATH, anio_objetivo)

        if not os.path.exists(carpeta_base):
            return render_template(
                "resultados.html",
                error=f"No se encontró la carpeta para el año {anio_objetivo}",
                producto=producto_objetivo,
                ciudad=ciudad_objetivo
            )

        archivos = [os.path.join(root, f) for root, _, files in os.walk(carpeta_base)
                    for f in files if f.lower().endswith((".xlsx", ".xls"))]

        if not archivos:
            return render_template(
                "resultados.html",
                error=f"No se encontraron archivos Excel en la carpeta {anio_objetivo}",
                producto=producto_objetivo,
                ciudad=ciudad_objetivo
            )

        dfs = []
        for path in tqdm(archivos, desc="Procesando archivos", unit="archivo"):
            df_temp = procesar_boletin(path, hoja, producto_objetivo, ciudad_objetivo)
            if not df_temp.empty:
                dfs.append(df_temp)

        if not dfs:
            return render_template(
                "resultados.html",
                error=f"No se encontró información de '{producto_objetivo}' en {ciudad_objetivo}.",
                producto=producto_objetivo,
                ciudad=ciudad_objetivo
            )

        df_final = pd.concat(dfs, ignore_index=True)
        df_final['fecha'] = pd.to_datetime(df_final['boletin'], errors='coerce')

        # Convertir columnas numéricas
        for col in ['precio_minimo', 'precio_maximo', 'precio_medio']:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

        # Limpiar datos inválidos
        filas_antes = len(df_final)
        df_final = df_final.dropna(subset=['fecha'])
        if 'precio_medio' in df_final.columns:
            df_final = df_final.dropna(subset=['precio_medio'])
        filas_despues = len(df_final)

        logger.info(f"Datos después de limpieza: {filas_despues}/{filas_antes}")

        if df_final.empty:
            return render_template(
                "resultados.html",
                error="No hay datos válidos después de la limpieza",
                producto=producto_objetivo,
                ciudad=ciudad_objetivo
            )

        # ======= Gráfico Mejorado =======
        fig = go.Figure()
        colores = px.colors.qualitative.Plotly
        mercados = df_final['mercado'].unique()
        color_map = {m: colores[i % len(colores)] for i, m in enumerate(mercados)}

        for mercado, grupo in df_final.groupby('mercado'):
            grupo = grupo.sort_values('fecha')

            # Línea de precios medios
            fig.add_trace(go.Scatter(
                x=grupo['fecha'],
                y=grupo['precio_medio'],
                mode='lines+markers',
                name=f"{mercado}",
                line=dict(color=color_map[mercado], width=3),
                marker=dict(size=6),
                hovertemplate=(
                        f"<b>{mercado}</b><br>" +
                        "Fecha: %{x|%d/%m/%Y}<br>" +
                        "Precio: %{y:,.0f} COP/kg<br>" +
                        "<extra></extra>"
                )
            ))

            # Tendencia suavizada - CORREGIDO
            if len(grupo) > 1:  # Solo calcular tendencia si hay suficientes puntos
                try:
                    # Convertir fechas a numérico para lowess
                    fechas_numeric = pd.to_numeric(grupo['fecha'])
                    y_suav = lowess(grupo['precio_medio'], fechas_numeric, frac=0.3, return_sorted=False)

                    fig.add_trace(go.Scatter(
                        x=grupo['fecha'],
                        y=y_suav,
                        mode='lines',
                        name=f"Tendencia {mercado}",
                        line=dict(color=color_map[mercado], width=2, dash='dot'),
                        hovertemplate=(
                                f"<b>Tendencia {mercado}</b><br>" +
                                "Fecha: %{x|%d/%m/%Y}<br>" +
                                "Tendencia: %{y:,.0f} COP/kg<br>" +
                                "<extra></extra>"
                        ),
                        showlegend=True
                    ))
                except Exception as e:
                    logger.warning(f"Error calculando tendencia para {mercado}: {e}")

        # Promedio anual global
        if 'precio_medio' in df_final.columns:
            promedio_anual = df_final['precio_medio'].mean()
            fig.add_trace(go.Scatter(
                x=[df_final['fecha'].min(), df_final['fecha'].max()],
                y=[promedio_anual, promedio_anual],
                mode='lines',
                name=f"Promedio anual: {promedio_anual:,.0f} COP/kg",
                line=dict(color='black', width=2, dash='dash'),
                hovertemplate=f'Promedio anual: {promedio_anual:,.0f} COP/kg<extra></extra>'
            ))

        # Layout responsivo y mejorado - FORMA CORRECTA
        fig.update_layout(
            title=dict(
                text=f"Evolución del precio del {producto_objetivo.title()} en {ciudad_objetivo.title()} ({anio_objetivo})",
                x=0.5,
                xanchor='center',
                font=dict(size=20)
            ),
            xaxis_title="Fecha del boletín",
            yaxis_title="Precio (COP/kg)",
            template="plotly_white",
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=600,
            margin=dict(l=50, r=50, t=80, b=50),
            # Configuración de ejes DENTRO de update_layout - CORREGIDO
            xaxis=dict(
                tickformat="%b %Y",
                tickangle=45
            ),
            yaxis=dict(
                tickformat=","
            )
        )

        graph_html = fig.to_html(
            full_html=False,
            include_plotlyjs=True,
            config={
                'responsive': True,
                'displayModeBar': True,
                'displaylogo': False,
                'modeBarButtonsToRemove': ['pan2d', 'lasso2d', 'select2d']
            }
        )

        # Estadísticas para el template
        stats = {
            'promedio_anual': df_final['precio_medio'].mean() if 'precio_medio' in df_final.columns else 0,
            'precio_max': df_final['precio_medio'].max() if 'precio_medio' in df_final.columns else 0,
            'precio_min': df_final['precio_medio'].min() if 'precio_medio' in df_final.columns else 0,
            'total_archivos': len(archivos),
            'archivos_procesados': len(dfs),
            'total_registros': len(df_final)
        }

        return render_template(
            "resultados.html",
            grafico=graph_html,
            producto=producto_objetivo,
            ciudad=ciudad_objetivo,
            **stats
        )

    except Exception as e:
        logger.error(f"Error en el análisis: {e}")
        return render_template(
            "resultados.html",
            error=f"Error en el procesamiento: {str(e)}",
            producto=request.form.get("producto", ""),
            ciudad=request.form.get("ciudad", "")
        )


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)