"""One style for every manuscript figure, so text and spacing look the same in the PDF.

The point is to draw each figure at the size it is printed. A figure drawn 13 inches wide and
placed at \\linewidth is shrunk by half, and its 9 pt labels end up at 4.5 pt; drawing it at the
printed width keeps the font sizes below exactly as set. So:

  * the figure width is TEXT_W times the fraction used in \\includegraphics[width=...\\linewidth];
  * all sizes come from the rcParams set in apply(); scripts should not pass their own fontsize;
  * layout is matplotlib's constrained layout with fixed paddings, so the gaps between title,
    panels and legend are the same in every figure;
  * save() writes at DPI without bbox_inches="tight", which would change the width.

    import paper_style as ps
    ps.apply()
    fig, axes = ps.subplots(1, 4, frac=1.0, height=2.4)
    ...
    ps.legend_below(fig, handles, ncol=4)
    ps.title(fig, "Optional figure title")
    ps.save(fig, path)

TEXT_W is the text width of the manuscript (\\the\\linewidth in Overleaf, in inches). If it
changes, change it here and regenerate; every figure follows.
"""
import matplotlib
import matplotlib.pyplot as plt

TEXT_W = 6.3          # inches; elsarticle preprint, 12pt
DPI = 400

FS = 8.0              # tick labels, axis labels, legend, annotations
FS_PANEL = 8.5        # panel (axes) titles
FS_TITLE = 9.5        # figure title

PAD = 0.04            # constrained-layout padding (inches) around and between panels
GAP = 0.02            # constrained-layout spacing between panels (fraction of the figure)


def apply():
    matplotlib.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": FS,
        "axes.titlesize": FS_PANEL,
        "axes.titleweight": "normal",
        "axes.titlepad": 4.0,
        "axes.labelsize": FS,
        "axes.labelpad": 3.0,
        "axes.linewidth": 0.6,
        "xtick.labelsize": FS,
        "ytick.labelsize": FS,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.major.size": 3.0,
        "ytick.major.size": 3.0,
        "xtick.major.pad": 2.5,
        "ytick.major.pad": 2.5,
        "legend.fontsize": FS,
        "legend.title_fontsize": FS,
        "legend.frameon": False,
        "legend.handlelength": 1.8,
        "legend.columnspacing": 1.4,
        "legend.borderaxespad": 0.3,
        "figure.titlesize": FS_TITLE,
        "figure.labelsize": FS,
        "lines.linewidth": 1.4,
        "grid.linewidth": 0.5,
        "figure.constrained_layout.use": True,
        "figure.constrained_layout.h_pad": PAD,
        "figure.constrained_layout.w_pad": PAD,
        "figure.constrained_layout.hspace": GAP,
        "figure.constrained_layout.wspace": GAP,
        "savefig.dpi": DPI,
        "savefig.bbox": None,
        "savefig.facecolor": "white",
    })


def subplots(nrows=1, ncols=1, frac=1.0, height=2.6, **kw):
    """Figure frac * TEXT_W wide (frac = the \\linewidth fraction in main.tex) and height inches."""
    kw.setdefault("layout", "constrained")
    return plt.subplots(nrows, ncols, figsize=(TEXT_W * frac, height), **kw)


def figure(frac=1.0, height=2.6, **kw):
    kw.setdefault("layout", "constrained")
    return plt.figure(figsize=(TEXT_W * frac, height), **kw)


def _legend(fig, handles, labels, loc, ncol, kw):
    if handles is not None:
        kw["handles"] = handles
    if labels is not None:
        kw["labels"] = labels
    return fig.legend(loc=loc, ncol=ncol, frameon=False, **kw)


def legend_below(fig, handles=None, labels=None, ncol=4, **kw):
    """Figure legend centred under all panels, at the standard gap."""
    return _legend(fig, handles, labels, "outside lower center", ncol, kw)


def legend_right(fig, handles=None, labels=None, ncol=1, **kw):
    return _legend(fig, handles, labels, "outside right center", ncol, kw)


def title(fig, text):
    return fig.suptitle(text, fontsize=FS_TITLE)


def save(fig, path, close=True):
    fig.savefig(path, dpi=DPI)
    if close:
        plt.close(fig)
    return path
