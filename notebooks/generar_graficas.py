import pandas as pd
from sqlalchemy import create_engine
from pyecharts import options as opts
from pyecharts.charts import Bar, Pie

def _get_db_engine(db_config):
    """Creates a SQLAlchemy engine from a dictionary of credentials."""
    db_url = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(db_url)

def _get_processed_data(engine):
    """Fetches and processes historical and prediction data."""
    query_hist = "SELECT TipoMantenimiento, FechaElaboracion AS Fecha, Debito AS Costo, 'Histórico' AS Origen FROM Hechos_Mantenimiento WHERE Debito > 0"
    query_pred = "SELECT TipoMantenimiento, Fecha, Costo, 'Predicción' AS Origen FROM Predicciones_Tipo_Mantenimiento"
    
    try:
        df_hist = pd.read_sql(query_hist, engine)
        df_pred = pd.read_sql(query_pred, engine)
    except Exception as e:
        print(f"Error al leer datos para gráficos de análisis: {e}")
        return pd.DataFrame()

    if df_hist.empty and df_pred.empty:
        return pd.DataFrame()

    df = pd.concat([df_hist, df_pred], ignore_index=True)
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df['AñoMes'] = df['Fecha'].dt.to_period('M').astype(str)
    
    return df.groupby(['TipoMantenimiento', 'Origen', 'AñoMes'])['Costo'].sum().reset_index()

def _create_monthly_comparison_chart(df_tipo, tipo):
    """Creates a comparative bar chart for a given maintenance type."""
    if df_tipo.empty:
        return (
            Bar(init_opts=opts.InitOpts(theme='light'))
            .set_global_opts(
                title_opts=opts.TitleOpts(title=f"Comparativo Mensual - {tipo}", subtitle="No hay datos disponibles"),
                xaxis_opts=opts.AxisOpts(type_="category"),
            )
        )

    df_hist = df_tipo[df_tipo['Origen'] == 'Histórico']
    df_pred = df_tipo[df_tipo['Origen'] == 'Predicción']
    
    all_months = sorted(list(set(df_tipo['AñoMes'])))

    bar = (
        Bar(init_opts=opts.InitOpts(theme='light'))
        .add_xaxis(all_months)
        .add_yaxis("Histórico", df_hist.set_index('AñoMes').reindex(all_months, fill_value=0)['Costo'].round(2).tolist())
        .add_yaxis("Predicción", df_pred.set_index('AñoMes').reindex(all_months, fill_value=0)['Costo'].round(2).tolist())
        .set_global_opts(
            title_opts=opts.TitleOpts(title=f"Comparativo Mensual - {tipo}"),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="shadow"),
            legend_opts=opts.LegendOpts(pos_left="right"),
            datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
            toolbox_opts=opts.ToolboxOpts(is_show=True) # <-- Caja de herramientas
        )
    )
    return bar

def _create_total_analysis_pie_chart(df_tipo, tipo):
    """Creates a pie chart summarizing historical vs. prediction costs."""
    if df_tipo.empty:
        return (
            Pie(init_opts=opts.InitOpts(theme='light'))
            .set_global_opts(
                title_opts=opts.TitleOpts(title=f"Análisis de Totales - {tipo}", subtitle="No hay datos disponibles"),
                legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%")
            )
        )

    totales = df_tipo.groupby('Origen')['Costo'].sum()
    data_pair = [list(z) for z in zip(totales.index, totales.values.round(2))]

    pie = (
        Pie(init_opts=opts.InitOpts(theme='light'))
        .add(
            series_name="Costo Total",
            data_pair=data_pair,
            radius=["40%", "75%"],
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=f"Análisis de Totales - {tipo}"),
            legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"),
        )
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))
    )
    return pie

def generate_analysis_charts(db_config):
    """Main function to generate and return all analysis charts."""
    engine = _get_db_engine(db_config)
    if not engine:
        return {}

    df_processed = _get_processed_data(engine)
    analysis_charts = {}

    # Obtener todos los tipos de mantenimiento posibles desde la tabla de dimensiones
    try:
        all_tipos = pd.read_sql("SELECT DISTINCT TipoMantenimiento FROM Dim_TipoMantenimiento", engine)['TipoMantenimiento'].tolist()
    except Exception as e:
        print(f"Advertencia: No se pudo obtener la lista completa de tipos de mantenimiento: {e}")
        all_tipos = ['CORRECTIVO', 'PREVENTIVO'] # Fallback a una lista conocida

    # Asegurar que se genere un grupo de gráficos para cada tipo de mantenimiento
    for tipo in all_tipos:
        df_tipo = df_processed[df_processed['TipoMantenimiento'] == tipo]
        
        # Aunque df_tipo esté vacío, las funciones de creación de gráficos devolverán un gráfico vacío con un mensaje.
        analysis_charts[tipo] = {
            'bar': _create_monthly_comparison_chart(df_tipo, tipo),
            'pie': _create_total_analysis_pie_chart(df_tipo, tipo)
        }
    
    return analysis_charts
