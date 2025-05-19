from setuptools import setup, find_packages

setup(
    name="estuaire_contrail",
    version="0.1.0",
    packages=find_packages(include=["scripts", "scripts.*"]),
    install_requires=[
        "pandas",
        "scikit-learn",
        "xgboost",
        "python-dotenv",
        "joblib",
        "loguru"
    ],
    include_package_data=True,
    description="Contrail impact prediction pipeline"
)
