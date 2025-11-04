# ============================
# 📊 PROCESADOR GENERAL DE BOLETINES SIPSA (con tendencia, promedio anual y análisis mensual)
# ============================

import pandas as pd
import numpy as np
import re, os, unicodedata
import plotly.graph_objects as go
from datetime import datetime
from statsmodels.nonparametric.smoothers_lowess import lowess
from tqdm import tqdm  # ✅ Barra de progreso elegante
import plotly.express as px

# Asegurar que kaleido esté disponible
try:
    import kaleido  # noqa
except ImportError:
    os.system("pip install -U kaleido")

# ============================
# ⚙️ CONFIGURACIÓN INICIAL
# ============================

anio_objetivo = input("📆 Ingrese el año a analizar (por ejemplo 2024 o 2025): ").strip()
carpeta_base = fr"E:\App3\datos\SIPSA_Historico\{anio_objetivo}"

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

print("\n📘 Secciones disponibles del boletín SIPSA:\n")
for k, v in opciones_hoja.items():
    print(f"   {k} → {v}")

hoja = input("\n📄 Ingrese el índice de la hoja a analizar (por ejemplo 1.1 o 1.4): ").strip()
if hoja not in opciones_hoja:
    print(f"⚠️ Índice no válido. Usando por defecto: 1.1 ({opciones_hoja['1.1']})")
    hoja = "1.1"

print(f"\n✅ Hoja seleccionada: {hoja} → {opciones_hoja[hoja]}\n")

producto_objetivo = input("🔎 Ingrese el nombre del producto (ej: tomate chonto, pimenton): ").strip()
ciudad_objetivo = input("🏙️ Ingrese la ciudad o mercado (ej: cali, corabastos): ").strip()


# ============================
# 🧩 FUNCIONES AUXILIARES
# ============================

def normalizar(texto):
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize('NFD', texto)
    texto = ''.join([c for c in texto if unicodedata.category(c) != 'Mn'])
    return texto.lower().strip()


def extraer_fecha(nombre_archivo):
    """Extrae fecha de nombres como: anex-SIPSASemanal-02ago08ago-2025.xlsx"""
    nombre = os.path.basename(nombre_archivo).lower()

    # Mapeo de meses
    meses = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }

    print(f"🔍 Procesando archivo: {nombre}")  # DEBUG

    # Patrón para formato: anex-SIPSASemanal-02ago08ago-2025.xlsx
    patrones = [
        r'(\d{1,2})([a-z]{3})(\d{1,2})([a-z]{3})-(\d{4})',  # 02ago08ago-2025
        r'(\d{1,2})([a-z]{3})-(\d{1,2})([a-z]{3})-(\d{4})',  # 02ago-08ago-2025
        r'(\d{1,2})([a-z]{3})(\d{1,2})([a-z]{3})_(\d{4})',  # 02ago08ago_2025
        r'(\d{4})[-_](\d{1,2})([a-z]{3})'  # 2025-02ago (formato alternativo)
    ]

    for i, patron in enumerate(patrones):
        m = re.search(patron, nombre)
        if m:
            print(f"✅ Patrón {i + 1} encontrado: {m.groups()}")  # DEBUG

            if len(m.groups()) == 5:
                # Formato: 02ago08ago-2025
                dia1, mes1, dia2, mes2, anio = m.groups()
                mes_num = meses.get(mes2, '01')
                fecha = f"{anio}-{mes_num}-{dia2.zfill(2)}"
                print(f"📅 Fecha extraída: {fecha}")  # DEBUG
                return fecha
            elif len(m.groups()) == 3:
                # Formato: 2025-02ago
                anio, dia, mes = m.groups()
                mes_num = meses.get(mes, '01')
                fecha = f"{anio}-{mes_num}-{dia.zfill(2)}"
                print(f"📅 Fecha extraída: {fecha}")  # DEBUG
                return fecha

    # Si no encuentra patrón, buscar solo el año
    m_anio = re.search(r'(\d{4})', nombre)
    if m_anio:
        anio = m_anio.group(1)
        fecha = f"{anio}-01-01"
        print(f"📅 Solo año encontrado: {fecha}")  # DEBUG
        return fecha

    print(f"❌ No se pudo extraer fecha de: {nombre}")
    return None


def procesar_boletin(path):
    print(f"\n📂 Procesando: {os.path.basename(path)}")

    df = None
    for header_row in range(5, 15):
        try:
            temp = pd.read_excel(path, sheet_name=hoja, header=header_row)
            columnas = [str(c).lower() for c in temp.columns]
            print(f"   🔍 Header {header_row}: {columnas[:3]}...")  # DEBUG

            if "producto" in columnas:
                df = temp
                print(f"   ✅ Header encontrado en fila {header_row}")
                break
        except Exception as e:
            continue

    if df is None:
        print(f"   ❌ No se pudo leer el archivo")
        return pd.DataFrame()

    df.columns = df.columns.str.strip().str.lower()
    print(f"   📊 Columnas: {df.columns.tolist()}")  # DEBUG

    if "producto" not in df.columns or "mercado mayorista" not in df.columns:
        print(f"   ❌ Columnas necesarias no encontradas")
        return pd.DataFrame()

    df["producto_norm"] = df["producto"].apply(normalizar)
    df["mercado_norm"] = df["mercado mayorista"].apply(normalizar)

    producto_buscado = normalizar(producto_objetivo)
    ciudad_buscada = normalizar(ciudad_objetivo)

    print(f"   🔎 Buscando: '{producto_buscado}' en '{ciudad_buscada}'")

    mask_prod = df["producto_norm"].str.contains(producto_buscado, na=False)
    mask_ciud = df["mercado_norm"].str.contains(ciudad_buscada, na=False)

    df_filtrado = df[mask_prod & mask_ciud].copy()

    if df_filtrado.empty:
        print(f"   ❌ No se encontró el producto en este archivo")
        # Mostrar algunos productos disponibles para debug
        productos_sample = df["producto_norm"].unique()[:3]
        mercados_sample = df["mercado_norm"].unique()[:3]
        print(f"   ℹ️  Productos disponibles: {productos_sample}")
        print(f"   ℹ️  Mercados disponibles: {mercados_sample}")
        return pd.DataFrame()

    # Renombrar columnas
    df_filtrado = df_filtrado.rename(columns={
        'mercado mayorista': 'mercado',
        'pesos por kilogramo': 'precio_minimo',
        'unnamed: 3': 'precio_maximo',
        'unnamed: 4': 'precio_medio'
    })

    cols = ['producto', 'mercado', 'precio_minimo', 'precio_maximo', 'precio_medio']
    df_filtrado = df_filtrado[[c for c in cols if c in df_filtrado.columns]]

    # Extraer fecha
    fecha = extraer_fecha(path)
    df_filtrado["boletin"] = fecha
    df_filtrado["archivo"] = os.path.basename(path)

    print(f"   ✅ Encontrados {len(df_filtrado)} registros")
    if len(df_filtrado) > 0:
        primer_registro = df_filtrado.iloc[0]
        print(
            f"   📝 Ejemplo: {primer_registro['producto']} - {primer_registro['mercado']} - ${primer_registro.get('precio_medio', 'N/A')}")

    return df_filtrado


# ============================
# 📂 1. BUSCAR ARCHIVOS
# ============================

print(f"\n🔍 Buscando archivos en: {carpeta_base}")
archivos = [os.path.join(root, f) for root, _, files in os.walk(carpeta_base)
            for f in files if f.lower().endswith((".xlsx", ".xls"))]
print(f"📦 {len(archivos)} archivos encontrados\n")

# ============================
# 🧠 2. PROCESAMIENTO MASIVO CON BARRA DE PROGRESO
# ============================

dfs = []
print("⏳ Procesando boletines...\n")

for i, path in enumerate(archivos, 1):
    print(f"\n--- Archivo {i}/{len(archivos)} ---")
    df_temp = procesar_boletin(path)
    if not df_temp.empty:
        dfs.append(df_temp)
        print(f"✅ Añadido al análisis")

print(f"\n{'=' * 50}")
print(f"📊 RESUMEN DEL PROCESAMIENTO")
print(f"{'=' * 50}")
print(f"📁 Archivos procesados: {len(dfs)}/{len(archivos)}")
print(f"📈 Registros encontrados: {sum(len(df) for df in dfs)}")

if not dfs:
    print(f"\n⚠️ No se encontró información de '{producto_objetivo}' en {ciudad_objetivo}.")
else:
    df_final = pd.concat(dfs, ignore_index=True)

    # Convertir tipos de datos
    df_final['fecha'] = pd.to_datetime(df_final['boletin'], errors='coerce')
    for col in ['precio_minimo', 'precio_maximo', 'precio_medio']:
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # Limpiar datos inválidos
    filas_antes = len(df_final)
    df_final = df_final.dropna(subset=['fecha', 'precio_medio'])
    filas_despues = len(df_final)

    print(f"🧹 Datos después de limpieza: {filas_despues}/{filas_antes}")
    print(f"📅 Rango de fechas: {df_final['fecha'].min()} a {df_final['fecha'].max()}")
    print(f"💰 Precio promedio: {df_final['precio_medio'].mean():.0f} COP/kg")

    if df_final.empty:
        print("❌ No hay datos válidos después de la limpieza")
    else:
        # ============================
        # 📈 VISUALIZACIÓN Y ANÁLISIS
        # ============================

        colores = px.colors.qualitative.Plotly
        mercados = df_final['mercado'].unique()
        color_map = {m: colores[i % len(colores)] for i, m in enumerate(mercados)}

        fig = go.Figure()

        for mercado, grupo in df_final.groupby('mercado'):
            grupo = grupo.sort_values('fecha')

            # Línea original
            fig.add_trace(go.Scatter(
                x=grupo['fecha'], y=grupo['precio_medio'],
                mode='lines+markers',
                name=f"{mercado} (Precio medio)",
                line=dict(color=color_map[mercado], width=2),
                hovertemplate='<b>%{x}</b><br>Precio: %{y:,.0f} COP/kg<extra></extra>'
            ))

            # Tendencia suavizada
            fechas_numeric = pd.to_numeric(grupo['fecha'])
            y_suav = lowess(grupo['precio_medio'], fechas_numeric, frac=0.4, return_sorted=False)

            fig.add_trace(go.Scatter(
                x=grupo['fecha'], y=y_suav,
                mode='lines', name=f"Tendencia {mercado}",
                line=dict(color=color_map[mercado], width=3, dash='dot'),
                hovertemplate='<b>%{x}</b><br>Tendencia: %{y:,.0f} COP/kg<extra></extra>'
            ))

        # Promedio anual global
        promedio_anual = df_final['precio_medio'].mean()
        fig.add_trace(go.Scatter(
            x=[df_final['fecha'].min(), df_final['fecha'].max()],
            y=[promedio_anual, promedio_anual],
            mode='lines',
            name=f"Promedio anual: {promedio_anual:,.0f} COP/kg",
            line=dict(color='black', width=2, dash='dash'),
            hovertemplate=f'Promedio anual: {promedio_anual:,.0f} COP/kg<extra></extra>'
        ))

        fig.update_layout(
            title=f"📊 Evolución del precio del {producto_objetivo.title()} en {ciudad_objetivo.title()} ({anio_objetivo})<br><sup>{opciones_hoja[hoja]}</sup>",
            xaxis_title="Fecha del boletín",
            yaxis_title="Precio (COP/kg)",
            template="plotly_white",
            hovermode="x unified",
            legend=dict(title="Capas del gráfico", bgcolor="rgba(255,255,255,0.7)"),
            height=600
        )

        print(f"\n🎨 Mostrando gráfico...")
        fig.show()

        # ============================
        # 📅 ANÁLISIS MENSUAL
        # ============================

        df_final['mes'] = df_final['fecha'].dt.month
        resumen_mensual = df_final.groupby('mes')['precio_medio'].mean().reset_index()

        if not resumen_mensual.empty:
            mes_max = resumen_mensual.loc[resumen_mensual['precio_medio'].idxmax()]
            mes_min = resumen_mensual.loc[resumen_mensual['precio_medio'].idxmin()]

            print("\n📆 ANÁLISIS DE PRECIOS POR MES:")
            print("=" * 40)
            for _, row in resumen_mensual.iterrows():
                print(f"   Mes {row['mes']:2d}: {row['precio_medio']:8.0f} COP/kg")
            print(f"\n🔺 Mes más caro:  Mes {mes_max['mes']} → {mes_max['precio_medio']:.0f} COP/kg")
            print(f"🔻 Mes más barato: Mes {mes_min['mes']} → {mes_min['precio_medio']:.0f} COP/kg")

        # Guardar gráfico
        try:
            salida = fr"E:\App3\datos\grafico_{producto_objetivo}_{ciudad_objetivo}_{anio_objetivo}.png"
            fig.write_image(salida)
            print(f"\n💾 Gráfico exportado en: {salida}")
        except Exception as e:
            print(f"⚠️ No se pudo exportar imagen: {e}")

print(f"\n{'=' * 50}")
print("🎯 PROCESO COMPLETADO")
print(f"{'=' * 50}")