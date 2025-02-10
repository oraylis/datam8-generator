from pytest_cases import parametrize


class CasesLayer:
    @parametrize("layer", ["raw", "stage", "core", "curated"])
    def case_layer_valid(self, layer):
        return layer


class CasesLocator:
    @parametrize(
        "locator",
        [
            # tuple consisting of a zone name and a valid locator
            ("raw", "/Raw/Sales/Customer/Customer_DE"),
            ("stage", "/stage/Sales/Product/Product"),
            ("stage", "stage/Sales/Product/Product"),
            ("raw", "/raw/Sales/Product/Product"),
            ("stage", "/stage/Sales/Customer/Customer_DE"),
            ("core", "/core/Sales/Customer/Customer"),
            ("curated", "/Curated/Sales/Customer/DimCustomer"),
            ("curated", "/curated/Sales/Customer/DimCustomer"),
            ("curated", "curated/Sales/Customer/DimCustomer"),
        ],
    )
    def case_locator_valid(self, locator):
        return locator

    @parametrize(
        "locator",
        [
            "//Sales/Product/Product",
            "/raw",
            "|raw|Sales|Pdouct|Product",
        ],
    )
    def case_locator_invalid(self, locator):
        return locator

    def case_locator_multiple(self):
        return "/"

    def case_locator_unkown(self):
        return "/stage/Sales/Delivery/Product"


class CasesModel:
    @parametrize(
        "attribute",
        [
            "lookup_entity",
            "perform_initial_checks",
        ],
    )
    def case_model_attributes(self, attribute):
        return attribute
