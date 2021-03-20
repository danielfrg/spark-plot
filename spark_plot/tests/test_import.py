def test_import():
    import spark_plot

    assert spark_plot.__version__ is not None
    assert spark_plot.__version__ != "0.0.0"
    assert len(spark_plot.__version__) > 0
