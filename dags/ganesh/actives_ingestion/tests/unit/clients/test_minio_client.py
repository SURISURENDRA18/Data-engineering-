from src.clients.minio_client import list_objects, list_processed_filenames, object_exists, read_object_bytes, route_file

BUCKET = "crushftp"


def test_list_objects_returns_keys_under_prefix(s3_client):
    s3_client.put_object(Bucket=BUCKET, Key="mediation/landing/a.csv", Body=b"1")
    s3_client.put_object(Bucket=BUCKET, Key="mediation/landing/b.csv", Body=b"22")
    s3_client.put_object(Bucket=BUCKET, Key="mediation/other/c.csv", Body=b"3")

    objects = list_objects(s3_client, BUCKET, "mediation/landing/")
    keys = {o["key"] for o in objects}
    assert keys == {"mediation/landing/a.csv", "mediation/landing/b.csv"}


def test_read_object_bytes(s3_client):
    s3_client.put_object(Bucket=BUCKET, Key="mediation/landing/a.csv", Body=b"hello")
    assert read_object_bytes(s3_client, BUCKET, "mediation/landing/a.csv") == b"hello"


def test_route_file_copies_without_deleting_source(s3_client):
    """landing/ is a shared pool other candidates' pipelines also read
    from -- routing a file must never remove the original."""
    s3_client.put_object(Bucket=BUCKET, Key="mediation/landing/a.csv", Body=b"hello")

    route_file(s3_client, BUCKET, "mediation/landing/a.csv", "mediation/validated/a.csv")

    assert object_exists(s3_client, BUCKET, "mediation/landing/a.csv") is True
    assert object_exists(s3_client, BUCKET, "mediation/validated/a.csv") is True
    assert read_object_bytes(s3_client, BUCKET, "mediation/validated/a.csv") == b"hello"


def test_route_file_is_idempotent_to_call_again(s3_client):
    s3_client.put_object(Bucket=BUCKET, Key="mediation/landing/a.csv", Body=b"hello")

    route_file(s3_client, BUCKET, "mediation/landing/a.csv", "mediation/validated/a.csv")
    route_file(s3_client, BUCKET, "mediation/landing/a.csv", "mediation/validated/a.csv")  # must not raise

    assert read_object_bytes(s3_client, BUCKET, "mediation/validated/a.csv") == b"hello"


def test_list_processed_filenames_covers_validated_and_rejected(s3_client):
    s3_client.put_object(Bucket=BUCKET, Key="mediation/validated/a.csv", Body=b"1")
    s3_client.put_object(Bucket=BUCKET, Key="mediation/rejected/zero_size/b.csv", Body=b"")
    s3_client.put_object(Bucket=BUCKET, Key="mediation/landing/c.csv", Body=b"3")  # not yet processed

    names = list_processed_filenames(s3_client, BUCKET, "mediation/validated/", "mediation/rejected/")

    assert names == {"a.csv", "b.csv"}
