# IMPORTING REQUIRED MODULES
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from stats_functions import *
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
FIGURES_DIR = BASE_DIR / "reports"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# PLOTS THE MONTHLY VOLUME OF A SINGLE COMPANY IN A GIVEN TIME PERIOD

def PlotVolumeCompany(company, start, end):
    df = GetCompanyVolumeSpan(company, start, end)
    plt.figure(figsize=(10, 6))
    sns.lineplot(x=df['Mes'], y=df['Volumen mensual'], data=df)
    plt.xlabel('Month')
    plt.ylabel('Sales Volume')
    plt.xticks(range(1, 13))  # ENSURE ALL MONTHS ARE DISPLAYED
    plt.grid(True)
    # FORMAT Y-AXIS TO DISPLAY NUMBERS WITHOUT SCIENTIFIC NOTATION AND WITH THOUSAND SEPARATORS
    formatter = ticker.FuncFormatter(lambda x, pos: f'{int(x):,}')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.savefig(FIGURES_DIR / f"VolumeCompany_{company}_{start}-{end}.png")
    plt.show()

# PLOTS THE MONTHLY VOLUME OF MULTIPLE COMPANIES IN A GIVEN TIME PERIOD

def PlotVolumeCompanies(companies, start, end):
    df = GetCompaniesVolumeSpan(companies, start, end)
    g = sns.relplot(
        data=df,
        x="Mes", y="Volumen mensual",
        hue="Empresa", kind="line",
        height=5, aspect=1.5
    )
    g.fig.suptitle(f"Monthly Volume by Company [{start} / {end}]")
    g.set_xlabels('Month')
    g.set_ylabels('Sales Volume')
    g.set_xticklabels(rotation=45)

    # FORMAT Y-AXIS
    formatter = ticker.FuncFormatter(lambda x, pos: f'{int(x):,}')
    g.axes[0][0].yaxis.set_major_formatter(formatter)

    plt.grid(True)
    g.fig.savefig(FIGURES_DIR / f"VolumeCompanies_{companies}_{start}-{end}.png", bbox_inches="tight")
    plt.show()

# PLOTS DAILY MOVING VOLATILITY OF MULTIPLE COMPANIES IN A GIVEN TIME PERIOD

def VMDailyGraphCompanies(companies, start_date, end_date):
    data_list = []
    for company in companies:
        company_data = VolatilidadMovil(company, start_date, end_date)
        data_list.append(company_data)

    df = pd.DataFrame(data_list)

    selected = df[[
        "Compañía", "Volatilidad diaria (20D)", "Volatilidad diaria (40D)",
        "Volatilidad diaria (60D)", "Volatilidad diaria (80D)", "Volatilidad diaria (100D)"
    ]]

    melted = selected.melt(
        id_vars="Compañía",
        var_name='Volatility Type',
        value_name='Value'
    )

    plt.figure(figsize=(10, 6))
    sns.barplot(data=melted, x='Volatility Type', y='Value', hue='Compañía', palette='viridis')
    plt.title('Daily Volatility Comparison by Company and Period')
    plt.ylabel('Volatility Value')
    plt.xlabel('Volatility Period')
    plt.xticks(rotation=15)
    plt.legend(title='Company')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / f"VMDaily_{companies}_{start_date}-{end_date}.png", bbox_inches="tight")
    plt.show()

# PLOTS YEARLY MOVING VOLATILITY OF MULTIPLE COMPANIES IN A GIVEN TIME PERIOD

def VMYearlyGraphCompanies(companies, start_date, end_date):
    data_list = []
    for company in companies:
        company_data = VolatilidadMovil(company, start_date, end_date)
        data_list.append(company_data)

    df = pd.DataFrame(data_list)

    selected = df[[
        "Compañía", "Volatilidad anualizada (20D)", "Volatilidad anualizada (40D)",
        "Volatilidad anualizada (60D)", "Volatilidad anualizada (100D)"
    ]]

    melted = selected.melt(
        id_vars="Compañía",
        var_name='Volatility Type',
        value_name='Value'
    )

    plt.figure(figsize=(10, 6))
    sns.barplot(data=melted, x='Volatility Type', y='Value', hue='Compañía', palette='viridis')
    plt.title('Yearly Volatility Comparison by Company and Period')
    plt.ylabel('Volatility Value')
    plt.xlabel('Volatility Period')
    plt.xticks(rotation=15)
    plt.legend(title='Company')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / f"VMYearly_{companies}_{start_date}-{end_date}.png", bbox_inches="tight")
    plt.show()

# PLOTS DAILY RETURNS OF MULTIPLE COMPANIES IN A GIVEN TIME PERIOD

def PlotRetornoCompanies(companies, start, end):
    df = GetVariousAdjRet(companies, start, end)

    if df.empty:
        print("NO DATA AVAILABLE FOR PLOTTING.")
        return

    g = sns.relplot(
        data=df,
        x="Fecha",
        y="Retorno diario",
        hue="Empresa",
        kind="line",
        height=5, aspect=2.5,
        errorbar=None
    )

    g.fig.suptitle(f"Daily Returns by Company [{start[0]}-{start[1]}-{start[2]} / {end[0]}-{end[1]}-{end[2]}]")
    g.set_xlabels('Date')
    g.set_ylabels('Daily Return')
    g.axes[0][0].grid(True)

    g.fig.savefig(FIGURES_DIR / f"Return_{companies}_{start}-{end}.png", bbox_inches="tight")
    plt.show()

# PLOTS CORRELATION MATRIX OF MULTIPLE COMPANIES IN A GIVEN TIME PERIOD

def PlotCorrInSpanCompanies(companies, start, end):
    df = GetCompaniesCorrInSpan(companies, start, end)

    plt.figure(figsize=(14, 7))
    sns.heatmap(df, fmt=".2f", cmap="coolwarm", annot=True, linewidths=.5, cbar=True)
    plt.title(f"Correlation Between Companies [{start[0]}-{start[1]}-{start[2]} / {end[0]}-{end[1]}-{end[2]}]")
    plt.grid(False)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / f"CorrInSpan_{companies}_{start}-{end}.png", bbox_inches="tight")
    plt.show()

# PLOTS DAILY RETURN DISTRIBUTION (HISTOGRAM) FOR MULTIPLE COMPANIES

def PlotHistRetornos(companies, start, end):
    df = GetVariousAdjRet(companies, start, end)

    if df.empty:
        print("NO DATA AVAILABLE FOR PLOTTING.")
        return

    f, ax = plt.subplots(figsize=(7, 5))
    sns.despine(f)
    sns.histplot(
        data=df, x="Retorno diario", hue="Empresa", kde=True,
        palette="tab10", edgecolor=".3", linewidth=.5, bins=50,
        alpha=0.5, multiple="stack"
    )
    plt.title(f"Daily Returns by Company [{start[0]}-{start[1]}-{start[2]} / {end[0]}-{end[1]}-{end[2]}]")
    plt.ylabel('Density')
    plt.xlabel('Daily Return')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / f"HistRetornos_{companies}_{start}-{end}.png", bbox_inches="tight")
    plt.show()
