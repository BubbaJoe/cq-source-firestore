package client

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/plugin-sdk/v3/schema"
	"github.com/cloudquery/plugin-sdk/v3/types"
	"google.golang.org/api/iterator"
)

func Identifier(name string) string {
	return "`" + name + "`"
}

type Stringer interface {
	String() string
}

func (c *Client) listTables(ctx context.Context, client *firestore.Client) (schema.Tables, error) {
	var schemaTables schema.Tables
	collections := client.Collections(ctx)
	for {
		collection, err := collections.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}

		parentTable := &schema.Table{
			Name: collection.ID,
			Columns: schema.ColumnList{
				{
					Name:       "__id",
					Type:       arrow.BinaryTypes.String,
					PrimaryKey: true,
					Unique:     true,
					NotNull:    true,
				},
				{Name: "__created_at", Type: arrow.FixedWidthTypes.Timestamp_us},
				{Name: "__updated_at", Type: arrow.FixedWidthTypes.Timestamp_us},
				{Name: "data", Type: types.ExtensionTypes.JSON},
			},
		}

		if c.nestedCollectionsTables {
			c.logger.Info().Msgf("Listing tables %s", collection.ID)
			newSchemaTables, err := c.addCollectionTables(ctx, collection.ID, collection, parentTable)
			if err != nil {
				return nil, err
			}
			schemaTables = append(schemaTables, newSchemaTables...)
		}

		schemaTables = append(schemaTables, parentTable)
	}
	c.logger.Info().Msgf("Found %d tables", len(schemaTables))
	return schemaTables, nil
}

func (c *Client) addCollectionTables(
	ctx context.Context,
	collectionId string,
	collection *firestore.CollectionRef,
	parentTable *schema.Table,
) (schema.Tables, error) {
	c.logger.Info().Msgf("Listing sub-tables of %s", collection.ID)
	schemaTables := schema.Tables{}
	docQuery := collection.Query.Limit(1000)
	docIter := docQuery.Documents(ctx)
	for {
		docSnap, err := docIter.Next()

		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		c.logger.Info().Msgf("Listing sub-tables of doc:%s", docSnap.Ref.ID)
		if !docSnap.Exists() {
			return schemaTables, nil
		}
		colIter := docSnap.Ref.Collections(ctx)
		for {
			nestedCol, err := colIter.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				return schemaTables, nil
			}
			newCollectionName := collectionId + "_" + nestedCol.ID
			// check if table already exists in schemaTables
			found := false
			for _, table := range schemaTables {
				if table.Name == newCollectionName {
					found = true
					break
				}
			}
			if found {
				continue
			}

			schemaTables = append(schemaTables, &schema.Table{
				Name: newCollectionName,
				Columns: schema.ColumnList{
					{
						Name:       "__id",
						Type:       arrow.BinaryTypes.String,
						PrimaryKey: true,
						Unique:     true,
						NotNull:    true,
					},
					{
						Name:       "__parent_id",
						Type:       arrow.BinaryTypes.String,
						PrimaryKey: true,
						Unique:     true,
						NotNull:    true,
					},
					{Name: "__created_at", Type: arrow.FixedWidthTypes.Timestamp_us},
					{Name: "__updated_at", Type: arrow.FixedWidthTypes.Timestamp_us},
					{Name: "data", Type: types.ExtensionTypes.JSON},
				},
			})
		}
	}
	return schemaTables, nil
}
