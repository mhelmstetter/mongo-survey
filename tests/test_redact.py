from mongo_survey.connection import redact_uri


def test_redacts_password():
    assert redact_uri("mongodb+srv://u:s3cr3t@c.example.net/?x=1") == "mongodb+srv://u:***@c.example.net/?x=1"


def test_encoded_at_sign_in_userinfo():
    assert redact_uri("mongodb://u:p%40ss@h1,h2/") == "mongodb://u:***@h1,h2/"


def test_no_credentials_unchanged():
    assert redact_uri("mongodb://localhost:27017") == "mongodb://localhost:27017"


def test_user_only():
    assert redact_uri("mongodb://u@h/?authMechanism=MONGODB-X509") == "mongodb://u@h/?authMechanism=MONGODB-X509"


def test_uri_file_must_hold_one_uri(tmp_path):
    import pytest
    from mongo_survey.connection import read_uri

    f = tmp_path / "u"
    f.write_text("mongodb://a\nmongodb://b\n")
    with pytest.raises(SystemExit, match="exactly one"):
        read_uri(None, str(f))
    f.write_text("\n  mongodb://a  \n\n")
    assert read_uri(None, str(f)) == "mongodb://a"
