import matplotlib.pyplot as plt
import pandas as pd
from typing import List, Tuple


class DataAnalysis(object):
    def plot_outliers_by_bins(self, df: pd.DataFrame, bins: int = 15, fields: List[str] = [], figsize: Tuple[int] = (20, 20)):
        fig, axes = plt.subplots(len(fields), 1, figsize=figsize)

        for index, f in enumerate(fields):
            cutted = pd.cut(df[f], right=False, bins=bins)
            labels = [f"{interval.left:.0f} to {interval.right:.0f}" for interval in cutted.cat.categories]

            cutted.value_counts().plot(
                kind='bar',
                ax=axes[index],
            )

            axes[index].set_xticklabels(labels, rotation=45, ha='right')
            axes[index].set_xlabel(f.upper(), labelpad=20, fontsize=12, loc='center')
            axes[index].xaxis.set_label_coords(0.5, 1.05)

        plt.tight_layout()
        plt.plot()


    def check_if_float_field_is_integer(self, df: pd.DataFrame, field: str) -> bool:
        calc = (df[field].astype(int) == df[field]).value_counts()
        return False not in calc.keys()
    

    def plot_pizza(
        self, value_counts: pd.DataFrame, labels: List[str], title: str,
        legend: str = '', ax: plt = None, colors: List[str] = None
    ):
        if ax is None:
            fig, ax = plt.subplots()

        ax.pie(
            value_counts,
            autopct='%1.1f%%',
            startangle=90,
            wedgeprops={'edgecolor': 'black'},
            colors=colors,
            # frame=ax,
            # textprops={'color': 'white'}
        )

        ax.set_title(title)

        if legend:
            ax.legend(
                labels,
                title=legend,
                bbox_to_anchor=(1.0, 1),
            )