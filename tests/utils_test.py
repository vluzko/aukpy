from aukpy import utils


def test_load_taxonomy():
    df = utils.load_taxonomy()
    assert len(df) > 0


def test_get_parent_taxon():
    assert utils.get_parent_taxon("Passeriformes") == "aves"
    assert utils.get_parent_taxon("Falconidae") == "falconiformes"
    assert utils.get_parent_taxon("Struthio") == "struthionidae"
