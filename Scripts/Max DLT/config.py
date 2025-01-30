# config space to store table references and transformation references

entity_configs = {
    "raw_payment_entities": {
        "source_tables": {
            "amer": {
                "max": "bolt_raw_prod.commerce.raw_any_amer_s2s_payment_transaction_entities",
                "dplus": None # Placeholder for missing table
            } ,
            "latam": {
                "max": "bolt_raw_prod.commerce.raw_any_latam_s2s_payment_transaction_entities"
            },
            "emea": {
                "max":"bolt_raw_prod.commerce.raw_any_emea_s2s_payment_transaction_entities"
            },
            "apac": {
                "max": "bolt_raw_prod.commerce.raw_any_apac_s2s_payment_transaction_entities"
            }
        },
        "final_transformations": "fs_payment_entities"  # Name of transformation function
    },
    #     "raw_payment_entities_v2": {
    #     "source_tables": {
    #         "amer": {
    #             "max": "bolt_raw_int.commerce.raw_any_amer_s2s_payment_transaction_v2_entities",
    #             "dplus": None # Placeholder for missing table
    #         } ,
    #         "latam": {
    #             "max": "bolt_raw_int.commerce.raw_any_latam_s2s_payment_transaction_v2_entities"
    #         },
    #         "emea": {
    #             "max":"bolt_raw_int.commerce.raw_any_emea_s2s_payment_transaction_v2_entities"
    #         },
    #         "apac": {
    #             "max": "bolt_raw_int.commerce.raw_any_apac_s2s_payment_transaction_v2_entities"
    #         }
    #     },
    #     "final_transformations": "fs_payment_entities_v2"  # Name of transformation function
    # },
    "raw_subscription_entities": {
        "source_tables": {
            "amer": {
                "max": "bolt_raw_prod.commerce.raw_beam_amer_s2s_subscription_entities",
                "dplus": None # Placeholder for missing table
            } ,
            "latam": {
                "max": "bolt_raw_prod.commerce.raw_beam_latam_s2s_subscription_entities"
            },
            "emea": {
                "max":"bolt_raw_prod.commerce.raw_beam_emea_s2s_subscription_entities"
            },
            "apac": {
                "max": "bolt_raw_prod.commerce.raw_beam_apac_s2s_subscription_entities"
            }
        },
        "final_transformations": "fs_subscription_entities"  # Name of transformation function
    },
      "raw_paymentmethod_entities": {
        "source_tables": {
            "amer": {
                "max": "bolt_raw_prod.commerce.raw_any_amer_s2s_payment_method_entities",
                "dplus": None # Placeholder for missing table
            } ,
            "latam": {
                "max": "bolt_raw_prod.commerce.raw_any_latam_s2s_payment_method_entities"
            },
            "emea": {
                "max": "bolt_raw_prod.commerce.raw_any_emea_s2s_payment_method_entities"
            },
            "apac": {
                "max": "bolt_raw_prod.commerce.raw_any_apac_s2s_payment_method_entities"
            }
        },
        "final_transformations": "fs_paymentmethod_entities"  # Name of transformation function
    },      
      "raw_user_entities": {
        "source_tables": {
            "amer": {
                "max": "bolt_raw_prod.consumer.raw_beam_amer_s2s_user_entities" ,
                "dplus": None # Placeholder for missing table
            } ,
            "latam": {
                "max": "bolt_raw_prod.consumer.raw_beam_latam_s2s_user_entities"
            },
            "emea": {
                "max": "bolt_raw_prod.consumer.raw_beam_emea_s2s_user_entities"
            },
            "apac": {
                "max": "bolt_raw_prod.consumer.raw_beam_apac_s2s_user_entities"
            }
        },
        "final_transformations": "fs_user_entities"  # Name of transformation function
    }
}

global_entity_configs = {     
    "raw_priceplan_entities": {
        "source_table": "bolt_raw_prod.commerce.raw_any_s2s_priceplan_entities",
        "final_transformations": "fs_priceplan_entities"  # Name of transformation function
    },      
    "raw_product_entities": {
        "source_table": "bolt_raw_prod.commerce.raw_any_s2s_product_entities",
        "final_transformations": "fs_product_entities"  # Name of transformation function
    },      
    "raw_campaign_entities": {
        "source_table": "bolt_raw_prod.commerce.raw_any_s2s_campaign_entities",
        "final_transformations": "fs_campaign_entities"  # Name of transformation function         
    }
}