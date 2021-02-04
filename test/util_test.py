import fhir_petl.util as util
import petl as etl


def test_dateparser():
    year = util.dateparser("%Y", util.ISOFormat.YEAR)
    assert year("1994").isoformat() == "1994"
    second = util.dateparser("%Y", util.ISOFormat.SECOND)
    assert second("1994").isoformat() == "1994-01-01T00:00:00"


def test_join():
    assert util.join() == ""
    assert util.join(1, 2, 3) == "1 2 3"
    assert util.join("1", 2, None) == "1 2"


def test_preprocess():
    header = ["SUBJECT", "NAME"]
    data = [["2", "1"], ["Steve", "Bob"]]
    table = etl.fromcolumns(data, header)

    table = util.preprocess(table, "SUBJECT")
    result = list(table.data())
    assert result == [(1, "Bob", result[0][2]), (2, "Steve", result[1][2])]
