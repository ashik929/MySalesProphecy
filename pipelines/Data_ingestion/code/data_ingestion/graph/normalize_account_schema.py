from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def normalize_account_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.withColumn("purchase-items", explode_outer("purchase.items")).columns
    selectCols = [col("account-created") if "account-created" in flt_col else col("account.created").alias("account-created"),                   col("account-status") if "account-status" in flt_col else col("account.status").alias("account-status"),                   col("account-subscription-renewal") if "account-subscription-renewal" in flt_col else col("account.subscription.renewal")\
                    .alias("account-subscription-renewal"),                   col("account-subscription-type") if "account-subscription-type" in flt_col else col("account.subscription.type")\
                    .alias("account-subscription-type"),                   col("login-ip") if "login-ip" in flt_col else col("login.ip").alias("login-ip"),                   col("paymentMethod-card") if "paymentMethod-card" in flt_col else col("paymentMethod.card")\
                    .alias("paymentMethod-card"),                   col("paymentMethod-email") if "paymentMethod-email" in flt_col else col("paymentMethod.email")\
                    .alias("paymentMethod-email"),                   col("paymentMethod-type") if "paymentMethod-type" in flt_col else col("paymentMethod.type")\
                    .alias("paymentMethod-type"),                   col("purchase-id") if "purchase-id" in flt_col else col("purchase.id").alias("purchase-id"),                   col("purchase-date") if "purchase-date" in flt_col else col("purchase.date").alias("purchase-date"),                   col("account-subscription-renewal") if "account-subscription-renewal" in flt_col else col("account.subscription.renewal")\
                    .alias("account-subscription-renewal"),                   col("account-subscription-type") if "account-subscription-type" in flt_col else col("account.subscription.type")\
                    .alias("account-subscription-type"),                   col("login-timestamp") if "login-timestamp" in flt_col else col("login.timestamp").alias("login-timestamp"),                   col("login-location-city") if "login-location-city" in flt_col else col("login.location.city")\
                    .alias("login-location-city"),                   col("login-location-country") if "login-location-country" in flt_col else col("login.location.country")\
                    .alias("login-location-country"),                   col("paymentMethod-card-brand") if "paymentMethod-card-brand" in flt_col else col("paymentMethod.card.brand")\
                    .alias("paymentMethod-card-brand"),                   col("paymentMethod-card-expiry-month") if "paymentMethod-card-expiry-month" in flt_col else col("paymentMethod.card.expiry.month")\
                    .alias("paymentMethod-card-expiry-month"),                   col("paymentMethod-card-expiry-year") if "paymentMethod-card-expiry-year" in flt_col else col("paymentMethod.card.expiry.year")\
                    .alias("paymentMethod-card-expiry-year"),                   col("paymentMethod-card-last4") if "paymentMethod-card-last4" in flt_col else col("paymentMethod.card.last4")\
                    .alias("paymentMethod-card-last4"),                   col("purchase-amount") if "purchase-amount" in flt_col else col("purchase.amount").alias("purchase-amount"),                   col("purchase-currency") if "purchase-currency" in flt_col else col("purchase.currency")\
                    .alias("purchase-currency"),                   col("purchase-items") if "purchase-items" in flt_col else col("purchase.items").alias("purchase-items"),                   col("purchase-items-name") if "purchase-items-name" in flt_col else col("purchase-items.name")\
                    .alias("purchase-items-name"),                   col("purchase-items-sku") if "purchase-items-sku" in flt_col else col("purchase-items.sku")\
                    .alias("purchase-items-sku"),                   col("purchase-items-qty") if "purchase-items-qty" in flt_col else col("purchase-items.qty")\
                    .alias("purchase-items-qty"),                   col("type") if "type" in flt_col else col("type"),                   col("user-contact") if "user-contact" in flt_col else col("user.contact").alias("user-contact"),                   col("user-email") if "user-email" in flt_col else col("user.email").alias("user-email"),                   col("user-id") if "user-id" in flt_col else col("user.id").alias("user-id"),                   col("user-contact-address") if "user-contact-address" in flt_col else col("user.contact.address")\
                    .alias("user-contact-address"),                   col("user-contact-address-city") if "user-contact-address-city" in flt_col else col("user.contact.address.city")\
                    .alias("user-contact-address-city"),                   col("user-contact-address-location-lat") if "user-contact-address-location-lat" in flt_col else col("user.contact.address.location.lat")\
                    .alias("user-contact-address-location-lat"),                   col("user-contact-address-location-lng") if "user-contact-address-location-lng" in flt_col else col("user.contact.address.location.lng")\
                    .alias("user-contact-address-location-lng"),                   col("user-contact-address-state") if "user-contact-address-state" in flt_col else col("user.contact.address.state")\
                    .alias("user-contact-address-state"),                   col("user-contact-address-street") if "user-contact-address-street" in flt_col else col("user.contact.address.street")\
                    .alias("user-contact-address-street"),                   col("user-contact-address-zip") if "user-contact-address-zip" in flt_col else col("user.contact.address.zip")\
                    .alias("user-contact-address-zip"),                   col("user-contact-phone-home") if "user-contact-phone-home" in flt_col else col("user.contact.phone.home")\
                    .alias("user-contact-phone-home"),                   col("user-contact-phone-work") if "user-contact-phone-work" in flt_col else col("user.contact.phone.work")\
                    .alias("user-contact-phone-work"),                   col("user-name-first") if "user-name-first" in flt_col else col("user.name.first").alias("user-name-first"),                   col("user-name-last") if "user-name-last" in flt_col else col("user.name.last").alias("user-name-last"),                   col("user-name-middle") if "user-name-middle" in flt_col else col("user.name.middle")\
                    .alias("user-name-middle"),                   col("user-preferences-notifications") if "user-preferences-notifications" in flt_col else col("user.preferences.notifications")\
                    .alias("user-preferences-notifications"),                   col("user-preferences-theme") if "user-preferences-theme" in flt_col else col("user.preferences.theme")\
                    .alias("user-preferences-theme"),                   col("user-preferences-notifications-email") if "user-preferences-notifications-email" in flt_col else col("user.preferences.notifications.email")\
                    .alias("user-preferences-notifications-email"),                   col(
                    "user-preferences-notifications-push-enabled"
                  ) if "user-preferences-notifications-push-enabled" in flt_col else col("user.preferences.notifications.push.enabled")\
                    .alias("user-preferences-notifications-push-enabled"),                   col(
                    "user-preferences-notifications-push-frequency"
                  ) if "user-preferences-notifications-push-frequency" in flt_col else col("user.preferences.notifications.push.frequency")\
                    .alias("user-preferences-notifications-push-frequency"),                   col("user-preferences-notifications-sms") if "user-preferences-notifications-sms" in flt_col else col("user.preferences.notifications.sms")\
                    .alias("user-preferences-notifications-sms")]

    return in0.withColumn("purchase-items", explode_outer("purchase.items")).select(*selectCols)
