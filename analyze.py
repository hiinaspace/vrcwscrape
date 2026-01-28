import marimo

__generated_with = "0.19.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.express as px
    from pathlib import Path

    DATA_DIR = Path("analytics")
    return DATA_DIR, mo, px


@app.cell
def _(DATA_DIR, mo):
    metrics_path = DATA_DIR / "world_metrics.parquet"
    worlds_path = DATA_DIR / "worlds.parquet"
    world_tags_path = DATA_DIR / "world_tags.parquet"
    missing_files = []
    if not metrics_path.exists():
        missing_files.append(str(metrics_path))
    if not worlds_path.exists():
        missing_files.append(str(worlds_path))
    if not world_tags_path.exists():
        missing_files.append(str(world_tags_path))
    mo.stop(
        bool(missing_files),
        mo.md("Missing parquet files: " + ", ".join(missing_files)),
    )
    return metrics_path, world_tags_path, worlds_path


@app.cell
def _(mo):
    top_n_slider = mo.ui.slider(
        start=10,
        stop=100,
        step=5,
        value=25,
        label="Top N worlds by visits",
    )
    weeks_back_slider = mo.ui.slider(
        start=4,
        stop=16,
        step=1,
        value=8,
        label="Weeks to include",
    )
    mo.hstack([top_n_slider, weeks_back_slider])
    return top_n_slider, weeks_back_slider


@app.cell
def _(metrics_path, mo, top_n_slider, worlds_path):
    top_worlds_df = mo.sql(
        f"""
        WITH latest_visits AS (
            SELECT
                world_id,
                max(visits) as visits
            FROM read_parquet('{metrics_path.as_posix()}')
            GROUP BY world_id
        )
        SELECT
            l.world_id,
            w.name,
            l.visits,
            coalesce(w.name, '(unknown)') AS label
        FROM latest_visits l
        LEFT JOIN read_parquet('{worlds_path.as_posix()}') w
            ON w.world_id = l.world_id
        ORDER BY l.visits DESC
        LIMIT {int(top_n_slider.value)}
        """
    )
    return (top_worlds_df,)


@app.cell
def _(px, top_worlds_df):
    top_worlds_fig = px.bar(
        top_worlds_df,
        x="visits",
        y="label",
        orientation="h",
        title="Top VRChat worlds by visits (all time)",
        labels={"visits": "Visits", "label": "World"},
    )
    top_worlds_fig.update_layout(
        height=600,
        yaxis={"categoryorder": "total ascending"},
        margin={"l": 10, "r": 10, "t": 60, "b": 10},
    )
    top_worlds_fig
    return


@app.cell
def _(mo, world_tags_path):
    top_tags_df = mo.sql(
        f"""
        SELECT
            tag,
            count(DISTINCT world_id) AS world_count
        FROM read_parquet('{world_tags_path.as_posix()}')
        GROUP BY tag
        ORDER BY world_count DESC
        LIMIT 25
        """
    )
    return (top_tags_df,)


@app.cell
def _(px, top_tags_df):
    top_tags_fig = px.bar(
        top_tags_df,
        x="world_count",
        y="tag",
        orientation="h",
        title="Top tags by world count",
        labels={"world_count": "World count", "tag": "Tag"},
    )
    top_tags_fig.update_layout(
        height=600,
        yaxis={"categoryorder": "total ascending"},
        margin={"l": 10, "r": 10, "t": 60, "b": 10},
    )
    top_tags_fig
    return


@app.cell
def _(metrics_path, mo, weeks_back_slider, worlds_path):
    weekly_top10_df = mo.sql(
        f"""
        WITH base AS (
            SELECT
                world_id,
                date_trunc('week', scrape_time) AS week_start,
                max(visits) AS week_visits_cum
            FROM read_parquet('{metrics_path.as_posix()}')
            GROUP BY world_id, date_trunc('week', scrape_time)
        ),
        with_delta AS (
            SELECT
                world_id,
                week_start,
                week_visits_cum,
                week_visits_cum
                    - lag(week_visits_cum) OVER (
                        PARTITION BY world_id ORDER BY week_start
                    ) AS weekly_visits
            FROM base
        ),
        recent_weeks AS (
            SELECT *
            FROM with_delta
            WHERE week_start >= (
                date_trunc('week', CURRENT_DATE) - INTERVAL '{int(weeks_back_slider.value)}' WEEK
            )
        ),
        ranked AS (
            SELECT
                world_id,
                week_start,
                COALESCE(weekly_visits, week_visits_cum) AS weekly_visits,
                row_number() OVER (
                    PARTITION BY week_start ORDER BY COALESCE(weekly_visits, week_visits_cum) DESC
                ) AS week_rank
            FROM recent_weeks
        ),
        top10 AS (
            SELECT * FROM ranked WHERE week_rank <= 10
        )
        SELECT
            t.week_start,
            t.week_rank,
            t.world_id,
            w.name,
            t.weekly_visits,
            coalesce(w.name, '(unknown)') AS label
        FROM top10 t
        LEFT JOIN read_parquet('{worlds_path.as_posix()}') w
            ON w.world_id = t.world_id
        ORDER BY t.week_start DESC, t.week_rank ASC
        """
    )
    return (weekly_top10_df,)


@app.cell
def _(metrics_path, mo, weekly_top10_df, weeks_back_slider, worlds_path):
    mo.stop(
        len(weekly_top10_df) == 0,
        mo.md("No weekly data available for the selected window."),
    )
    weekly_series_df = mo.sql(
        f"""
        WITH base AS (
            SELECT
                world_id,
                date_trunc('week', scrape_time) AS week_start,
                max(visits) AS week_visits_cum
            FROM read_parquet('{metrics_path.as_posix()}')
            GROUP BY world_id, date_trunc('week', scrape_time)
        ),
        with_delta AS (
            SELECT
                world_id,
                week_start,
                week_visits_cum,
                week_visits_cum
                    - lag(week_visits_cum) OVER (
                        PARTITION BY world_id ORDER BY week_start
                    ) AS weekly_visits
            FROM base
        ),
        recent_weeks AS (
            SELECT *
            FROM with_delta
            WHERE week_start >= (
                date_trunc('week', CURRENT_DATE) - INTERVAL '{int(weeks_back_slider.value)}' WEEK
            )
        ),
        ranked AS (
            SELECT
                world_id,
                week_start,
                COALESCE(weekly_visits, week_visits_cum) AS weekly_visits,
                row_number() OVER (
                    PARTITION BY week_start ORDER BY COALESCE(weekly_visits, week_visits_cum) DESC
                ) AS week_rank
            FROM recent_weeks
        ),
        top10_worlds AS (
            SELECT DISTINCT world_id FROM ranked WHERE week_rank <= 10
        )
        SELECT
            r.week_start,
            r.world_id,
            w.name,
            r.weekly_visits,
            coalesce(w.name, '(unknown)') AS label
        FROM ranked r
        JOIN top10_worlds t
            ON r.world_id = t.world_id
        LEFT JOIN read_parquet('{worlds_path.as_posix()}') w
            ON w.world_id = r.world_id
        ORDER BY r.week_start ASC, r.weekly_visits DESC
        """
    )
    return (weekly_series_df,)


@app.cell
def _(px, weekly_series_df):
    weekly_series_fig = px.line(
        weekly_series_df,
        x="week_start",
        y="weekly_visits",
        color="label",
        title="Weekly visits for worlds that made the top 10",
        labels={
            "week_start": "Week",
            "weekly_visits": "Weekly visits",
            "label": "World",
        },
        markers=True,
    )
    weekly_series_fig.update_layout(
        height=600,
        margin={"l": 10, "r": 10, "t": 60, "b": 10},
    )
    weekly_series_fig
    return


@app.cell
def _(mo, weekly_top10_df):
    mo.md("### Top 10 worlds by weekly visits (recent weeks)")
    weekly_top10_df
    return


if __name__ == "__main__":
    app.run()
