from aukpy import utils


def test_load_taxonomy():
    df = utils.load_taxonomy()
    assert len(df) > 0


def test_get_parent_taxon():
    assert utils.get_parent_taxon("Passeriformes") == "aves"
    assert utils.get_parent_taxon("Falconidae") == "falconiformes"
    assert utils.get_parent_taxon("Struthio") == "struthionidae"


def test_get_child_taxa():
    assert utils.get_child_taxa("Falconiformes") == ["falconidae"]
    assert utils.get_child_taxa("falconidae") == [
        "micrastur",
        "daptrius",
        "ibycter",
        "phalcoboenus",
        "caracara",
        "milvago",
        "herpetotheres",
        "spiziapteryx",
        "polihierax",
        "microhierax",
        "falco",
    ]
    assert utils.get_child_taxa("gaviidae") == ["gavia"]
    assert utils.get_child_taxa("gavia") == [
        "stellata",
        "arctica",
        "pacifica",
        "immer",
        "adamsii",
    ]
