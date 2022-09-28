from typing import List, Optional
import pandas as pd

from aukpy import config


def load_taxonomy() -> pd.DataFrame:
    """Load the taxonomy file"""
    return pd.read_csv(config.TAXONOMY_CSV)


def load_bcr() -> pd.DataFrame:
    """Load the BCR codes"""
    return pd.read_csv(config.BCR_CODES, sep="\t")


def load_iba() -> pd.DataFrame:
    """Load IBA codes"""
    return pd.read_csv(config.IBA_CODES, sep="\t")


def load_usfws() -> pd.DataFrame:
    """Load the USFWS codes"""
    return pd.read_csv(config.USFWS_CODES, sep="\t")


def get_all_species(clade_name: str) -> pd.DataFrame:
    """Get all species in a clade"""
    taxonomy = load_taxonomy()
    raise NotImplementedError


def get_parent_taxon(name: str) -> Optional[str]:
    """Given the name of a taxon, get the parent taxon.
    Performance could be improved by caching more of the taxonomy but there's not much point yet.

    Examples:
        >>> get_parent_taxon("Passeriformes")
        "Aves"
        >>> get_parent_taxon("falconidae")
        "falconiformes"
        >>> get_parent_taxon("struthio")
        "struthionidae"
    """
    fmt_name = name.lower().strip()
    taxonomy = load_taxonomy()
    # Check orders
    orders = set(taxonomy["order"].unique())
    if fmt_name in orders:
        return "aves"
    else:
        # Check family
        f_to_o = taxonomy.groupby("family")["order"].first()
        if fmt_name in f_to_o:
            return f_to_o[fmt_name]
        else:
            # Check genus
            # We ignore the SettingWithCopyWarning because we're throwing out dataframe immediately.
            # Copying it would be a waste.
            pd.options.mode.chained_assignment = None  # type: ignore
            proper_species = taxonomy.loc[taxonomy["category"] == "species", :]
            proper_species["genus"] = proper_species["sci_name"].str.split(" ").str[0]
            g_to_f = proper_species.groupby("genus")["family"].first()
            return g_to_f.get(fmt_name, None)  # type: ignore


def get_child_taxa(name: str) -> List[str]:
    """Given the name of a taxon, get the child taxa.

    Examples:
        >>> get_child_taxa("Falconiformes")
        ["Falconidae"]
    """
    taxonomy = load_taxonomy()
    # Check orders
    orders = taxonomy.groupby("orders")["family"].unique()
    import pdb

    pdb.set_trace()
    raise NotImplementedError
