using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.ProviderHelpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    public static class MappingExtensions
    {
        public static bool IsKeyColumnExists(this IEnumerable<ColumnMapping> mappings)
        {
            bool isPrimaryKeyColumnExists = GetKeyColumnMappings(mappings).Any();
            if (!isPrimaryKeyColumnExists)
                isPrimaryKeyColumnExists = mappings.Any(cm => cm.Active && ((SqlColumn)cm.DestinationColumn).IsPrimaryKey);
            return isPrimaryKeyColumnExists;
        }

        public static IEnumerable<ColumnMapping> GetKeyColumnMappings(this IEnumerable<ColumnMapping> mappings)
        {
            return mappings.Where(cm => cm != null && cm.DestinationColumn != null && cm.Active && cm.IsKey);
        }

        public static bool IsKeyColumn(this Column column, IEnumerable<ColumnMapping> mappings)
        {
            var keyColumnMappings = GetKeyColumnMappings(mappings);
            if (keyColumnMappings.Any())
            {
                return keyColumnMappings.Any(cm => cm.DestinationColumn == column);
            }
            return column.IsPrimaryKey;
        }

        public static string GetConditionalsSql(out List<SqlParameter> parameters, MappingConditionalCollection conditionals, bool skipVirtualColumns, bool useDestinationColumns)
        {
            string conditionalsSql = string.Empty;
            int conditionalCount = 0;
            parameters = new List<SqlParameter>();

            foreach (MappingConditional conditional in conditionals)
            {
                if (skipVirtualColumns && conditional.SourceColumn != null && conditional.Mapping != null && conditional.Mapping.DestinationTable != null
                    && conditional.Mapping.DestinationTable.Columns.Where(c => c.IsNew).Any(virtualColumn => string.Compare(virtualColumn.Name, conditional.SourceColumn.Name, true) == 0))
                {
                    continue;
                }
                string columnName = conditional.SourceColumn.Name;
                if (useDestinationColumns)
                {
                    ColumnMapping columnMapping = conditional.Mapping.GetColumnMappings().FirstOrDefault(cm => cm != null && cm.SourceColumn != null && string.Compare(cm.SourceColumn.Name, conditional.SourceColumn.Name, true) == 0);
                    if (columnMapping != null && columnMapping.DestinationColumn != null)
                    {
                        columnName = columnMapping.DestinationColumn.Name;
                    }
                    else
                    {
                        continue;
                    }
                }

                conditionalsSql = GetConditionalSql(conditionalsSql, columnName, conditional, conditionalCount);

                if (conditional.SourceColumn.Type == typeof(DateTime))
                {
                    parameters.Add(new SqlParameter("@conditional" + conditionalCount, DateTime.Parse(conditional.Condition)));
                }
                else
                {
                    parameters.Add(new SqlParameter("@conditional" + conditionalCount, conditional.Condition));
                }

                conditionalCount = conditionalCount + 1;
            }

            return conditionalsSql;
        }

        public static string GetConditionalSql(string conditionalsSql, string columnName, MappingConditional mappingConditional, int conditionalCount)
        {
            switch (mappingConditional.ConditionalOperator)
            {
                case ConditionalOperator.Contains:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] LIKE '%{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.NotContains:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT LIKE '%{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.EqualTo:
                    if (mappingConditional.IsNullStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}]] IS NULL and ", conditionalsSql, columnName);
                        break;
                    }
                    else if (mappingConditional.IsNullOrEmptyStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}] IS NULL OR [{2}] = '' and ", conditionalsSql, columnName, columnName);
                        break;
                    }
                    else
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] = @conditional{2} and ",
                            conditionalsSql, columnName, conditionalCount
                        );
                    }
                    break;

                case ConditionalOperator.DifferentFrom:
                    if (mappingConditional.IsNullStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}] IS NOT NULL and ", conditionalsSql, columnName);
                        break;
                    }
                    else if (mappingConditional.IsNullOrEmptyStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}] IS NOT NULL AND [{1}] <> '' and ", conditionalsSql, columnName);
                        break;
                    }
                    else
                    {
                        conditionalsSql = string.Format("{0}[{1}] <> @conditional{2} and ", conditionalsSql, columnName, conditionalCount);
                    }
                    break;

                case ConditionalOperator.In:
                    var conditionalValue = mappingConditional.Condition;
                    if (!string.IsNullOrEmpty(conditionalValue))
                    {
                        if (mappingConditional.SourceColumn.Type == typeof(string))
                        {
                            conditionalValue = string.Join(",", conditionalValue.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(val => $"'{val.Trim()}'"));
                        }
                        conditionalsSql = string.Format(
                            "{0}[{1}] IN ({2}) and ",
                            conditionalsSql,
                            columnName,
                            conditionalValue
                        );
                    }
                    break;

                case ConditionalOperator.NotIn:
                    var notInConditionalValue = mappingConditional.Condition;
                    if (!string.IsNullOrEmpty(notInConditionalValue))
                    {
                        if (mappingConditional.SourceColumn.Type == typeof(string))
                        {
                            notInConditionalValue = string.Join(",", notInConditionalValue.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(val => $"'{val.Trim()}'"));
                        }
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT IN ({2}) and ",
                            conditionalsSql,
                            columnName,
                            notInConditionalValue
                        );
                    }
                    break;

                case ConditionalOperator.StartsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] LIKE '{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.NotStartsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT LIKE '{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.EndsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] LIKE '%{2}' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.NotEndsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT LIKE '%{2}' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.GreaterThan:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        if (mappingConditional.SourceColumn.Type == typeof(string) || mappingConditional.SourceColumn.Type == typeof(DateTime))
                        {
                            conditionalsSql = string.Format("{0}[{1}] > '{2}' and ", conditionalsSql, columnName, mappingConditional.Condition);
                        }
                        else
                        {
                            conditionalsSql = string.Format("{0}[{1}] > {2} and ", conditionalsSql, columnName, mappingConditional.Condition);
                        }
                    }
                    break;

                case ConditionalOperator.LessThan:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        if (mappingConditional.SourceColumn.Type == typeof(string) || mappingConditional.SourceColumn.Type == typeof(DateTime))
                        {
                            conditionalsSql = string.Format("{0}[{1}] < '{2}' and ", conditionalsSql, columnName, mappingConditional.Condition);
                        }
                        else
                        {
                            conditionalsSql = string.Format("{0}[{1}] < {2} and ", conditionalsSql, columnName, mappingConditional.Condition);
                        }
                    }
                    break;

                default:
                    conditionalsSql = string.Format(
                        "{0}[{1}] = @conditional{2} and ",
                        conditionalsSql,
                        columnName,
                        conditionalCount
                    );
                    break;
            }

            return conditionalsSql;
        }
        public static string GetShopIdColumnName(string databaseTableName)
        {
            string result;
            switch (databaseTableName)
            {
                case "AccessUser":
                    result = "AccessUserShopId";
                    break;
                case "Area":
                    result = "AreaEcomShopId";
                    break;
                case "EcomAssortmentShopRelations":
                    result = "AssortmentShopRelationShopID";
                    break;
                case "EcomDiscount":
                    result = "DiscountShopId";
                    break;
                case "EcomFeed":
                    result = "FeedChannelId";
                    break;
                case "EcomFees":
                    result = "FeeShopId";
                    break;
                case "EcomLoyaltyRewardRule":
                    result = "LoyaltyRewardRuleShopId";
                    break;
                case "EcomOrderContextShopRelation":
                    result = "OrderContextShopRelationShopId";
                    break;
                case "EcomOrderLineFieldGroupRelation":
                    result = "OrderLineFieldGroupRelationShopID";
                    break;
                case "EcomOrders":
                    result = "OrderShopId";
                    break;
                case "EcomPrices":
                    result = "PriceShopId";
                    break;
                case "EcomProductAssignmentRuleShop":
                    result = "EcomProductAssignmentRuleShopShopId";
                    break;
                case "EcomProducts":
                    result = "ProductDefaultShopId";
                    break;
                case "EcomSalesDiscount":
                    result = "SalesDiscountShop";
                    break;
                case "EcomShopGroupRelation":
                    result = "ShopGroupShopId";
                    break;
                case "EcomShopLanguageRelation":
                    result = "ShopId";
                    break;
                case "EcomShops":
                    result = "ShopId";
                    break;
                case "EcomShopStockLocationRelation":
                    result = "ShopRelationShopId";
                    break;
                case "TemplateMenu":
                    result = "TemplateMenuEcomShopId";
                    break;
                default:
                    result = "";
                    break;
            }
            return result;
        }

        public static string GetLanguageIdColumnName(string databaseTableName)
        {
            string result;
            switch (databaseTableName)
            {
                case "AccessUserCard":
                    result = "AccessUserCardLanguageID";
                    break;
                case "Area":
                    result = "AreaEcomLanguageId";
                    break;
                case "EcomAssortmentItems":
                    result = "AssortmentItemLanguageID";
                    break;
                case "EcomAssortments":
                    result = "AssortmentLanguageID";
                    break;
                case "EcomCountryText":
                    result = "CountryTextLanguageId";
                    break;
                case "EcomCurrencies":
                    result = "CurrencyLanguageId";
                    break;
                case "EcomCustomerFavoriteProducts":
                    result = "ProductLanguageId";
                    break;
                case "EcomDetails":
                    result = "DetailLanguageId";
                    break;
                case "EcomDetailsGroupTranslation":
                    result = "DetailsGroupTranslationLanguageId";
                    break;
                case "EcomDiscount":
                    result = "DiscountLanguageId";
                    break;
                case "EcomDiscountTranslation":
                    result = "DiscountTranslationLanguageId";
                    break;
                case "EcomFieldDisplayGroupTranslation":
                    result = "FieldDisplayGroupTranslationLanguageId";
                    break;
                case "EcomFieldOptionTranslation":
                    result = "EcomFieldOptionTranslationLanguageID";
                    break;
                case "EcomGroups":
                    result = "GroupLanguageId";
                    break;
                case "EcomLanguages":
                    result = "LanguageId";
                    break;
                case "EcomLoyaltyRewardTranslation":
                    result = "LoyaltyRewardTranslationLanguageId";
                    break;
                case "EcomMethodCountryRelation":
                    result = "MethodCountryRelLanguageId";
                    break;
                case "EcomOrders":
                    result = "OrderLanguageId";
                    break;
                case "EcomOrderStateTranslations":
                    result = "OrderStateTranslationLanguageId";
                    break;
                case "EcomPayments":
                    result = "PaymentLanguageId";
                    break;
                case "EcomProductCategoryFieldGroupValue":
                    result = "FieldValueGroupLanguageId";
                    break;
                case "EcomProductCategoryFieldTranslation":
                    result = "FieldTranslationLanguageId";
                    break;
                case "EcomProductCategoryTranslation":
                    result = "CategoryTranslationLanguageId";
                    break;
                case "EcomProductFieldTranslation":
                    result = "ProductFieldTranslationLanguageID";
                    break;
                case "EcomProducts":
                    result = "ProductLanguageId";
                    break;
                case "EcomProductsRelatedGroups":
                    result = "RelatedGroupLanguageId";
                    break;
                case "EcomRelatedSmartSearches":
                    result = "RelatedLanguageId";
                    break;
                case "EcomRmaEmailConfigurations":
                    result = "RmaEmailConfigurationLanguage";
                    break;
                case "EcomRmaEventTranslations":
                    result = "RmaEventTranslationLanguageId";
                    break;
                case "EcomRmas":
                    result = "RmaEmailNotificationLanguage";
                    break;
                case "EcomRmaStateTranslations":
                    result = "RmaStateLanguageId";
                    break;
                case "EcomSalesDiscountLanguages":
                    result = "SalesDiscountLanguagesLanguageId";
                    break;
                case "EcomSavedForLater":
                    result = "SavedForLaterLanguageId";
                    break;
                case "EcomShippings":
                    result = "ShippingLanguageId";
                    break;
                case "EcomShopLanguageRelation":
                    result = "LanguageId";
                    break;
                case "EcomStockLocation":
                    result = "StockLocationLanguageId";
                    break;
                case "EcomStockStatusLanguageValue":
                    result = "StockStatusLanguageValueLanguageId";
                    break;
                case "EcomValidationGroupsTranslation":
                    result = "EcomValidationGroupsTranslationValidationGroupLanguageID";
                    break;
                case "EcomVariantGroupOptionPropertyValue":
                    result = "VariantGroupOptionPropertyValueLanguageID";
                    break;
                case "EcomVariantGroups":
                    result = "VariantGroupLanguageId";
                    break;
                case "EcomVariantsOptions":
                    result = "VariantOptionLanguageId";
                    break;
                case "EcomVatCountryRelations":
                    result = "VatCountryRelLangId";
                    break;
                case "EcomVatGroups":
                    result = "VatGroupLanguageId";
                    break;
                case "Languages":
                    result = "LanguageID";
                    break;
                default:
                    result = "";
                    break;
            }
            return result;
        }
    }
}