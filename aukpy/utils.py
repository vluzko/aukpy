from typing import List, Optional, Set
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


def get_all_species(clade_name: str) -> Set[str]:
    """Get all species in a clade"""
    taxonomy = load_taxonomy()
    relevant = taxonomy[taxonomy["category"] == "species"]
    fmt_name = clade_name.lower().strip()

    if fmt_name in relevant["order"].values:
        filt = relevant["order"] == fmt_name
    elif fmt_name in relevant["family"].values:
        filt = relevant["family"] == fmt_name
        # return list(relevant.loc[filt, "sci_name"].unique())  # type: ignore
    else:
        g_and_s = taxonomy["sci_name"].str.split(" ")
        taxonomy["genus"] = g_and_s.str[0]
        taxonomy["species"] = g_and_s.str[1]
        relevant = taxonomy[taxonomy["category"] == "species"]

        if fmt_name in relevant["genus"].values:
            filt = relevant["genus"] == fmt_name
        else:
            return set()

    species = relevant.loc[filt, "sci_name"].unique()
    return set(species)  # type: ignore


def get_parent_taxon(name: str) -> Optional[str]:
    """Given the name of a taxon, get the parent taxon.
    Performance could be improved by caching more of the taxonomy but there's not much point yet.

    Args:
        name: The name of the taxon. Case-insensitive.

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
            prev = pd.options.mode.chained_assignment
            pd.options.mode.chained_assignment = None  # type: ignore
            proper_species = taxonomy.loc[taxonomy["category"] == "species", :]
            proper_species["genus"] = proper_species["sci_name"].str.split(" ").str[0]
            g_to_f = proper_species.groupby("genus")["family"].first()
            # Return to previous setting
            pd.options.mode.chained_assignment = prev
            return g_to_f.get(fmt_name, None)  # type: ignore


def get_child_taxa(name: str) -> Optional[List[str]]:
    """Given the name of a taxon, get the child taxa.

    Args:
        name: The name of the taxon. Case-insensitive

    Examples:
        >>> get_child_taxa("Falconiformes")
        ["falconidae"]
        >>> get_child_taxa("gaviidae")
        ["gavia"]
        >>> get_child_taxa('gavia')
        ['stellata', 'arctica', 'pacifica', 'immer', 'adamsii']
    """
    taxonomy = load_taxonomy()
    fmt_name = name.lower().strip()
    # Check orders
    orders = taxonomy.groupby("order")["family"].unique()
    if fmt_name in orders:
        return list(orders[fmt_name])
    else:
        # Check family
        g_and_s = taxonomy["sci_name"].str.split(" ")
        taxonomy["genus"] = g_and_s.str[0]
        taxonomy["species"] = g_and_s.str[1]
        relevant = taxonomy[taxonomy["category"] == "species"]

        f_to_g = relevant.groupby("family")["genus"].unique()
        if fmt_name in f_to_g:
            return list(f_to_g[fmt_name])
        else:
            # Check genus
            g_to_s = relevant.groupby("genus")["species"].unique()
            res = g_to_s.get(fmt_name, None)
            if res is not None:
                return list(res)  # type: ignore
            else:
                return None
