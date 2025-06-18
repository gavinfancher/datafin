import plotly.graph_objects as go

from .datetime_tools import format_date


def generate_chart(
        df,
        title=None
):
        
    fig = go.Figure(data=[
            go.Candlestick(
                x=df['datetime_ny'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close']
            )
        ]
    )
    
    previous_close_value = df['previous_close'].iloc[0]

    fig.add_hline(
    y=previous_close_value,
    line_color="blue",
    line_width=1
    )


    df = df.reset_index(drop=True)
    date = format_date(df['datetime_ny'].head(1)[0].date())
    if title:
        fig.update_layout(
        title={
            'text': f'{title} | {date}',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 15,
                    'color': 'black'
            }
        }
)

    fig.update_layout(xaxis_rangeslider_visible=False)
        
    image_bytes = fig.to_image(
        format="png",
        scale=5,
        engine="kaleido"
    )

    return image_bytes