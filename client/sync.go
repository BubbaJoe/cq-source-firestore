package client

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/cloudquery/plugin-sdk/v3/plugins/source"
	"github.com/cloudquery/plugin-sdk/v3/schema"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

func (c *Client) Sync(ctx context.Context, metrics *source.Metrics, res chan<- *schema.Resource) error {
	c.metrics = metrics
	for _, table := range c.Tables {
		if c.metrics.TableClient[table.Name] == nil {
			c.metrics.TableClient[table.Name] = make(map[string]*source.TableClientMetrics)
			c.metrics.TableClient[table.Name][c.ID()] = &source.TableClientMetrics{}
		}
	}
	return c.syncTables(ctx, res)
}

func (c *Client) syncTable(ctx context.Context, table *schema.Table, res chan<- *schema.Resource) error {
	lastDocumentId := ""
	maxBatchSize := c.maxBatchSize
	eg, ctx := errgroup.WithContext(ctx)
	collection := c.client.Collection(table.Name)
	c.logger.Info().Msgf("Syncing table %s", table.Name)
	for {
		orderBy := firestore.DocumentID
		if c.orderBy != "" {
			orderBy = c.orderBy
		}

		dir := firestore.Asc
		if c.orderDirection == "desc" {
			dir = firestore.Desc
		}
		query := collection.Query.
			OrderBy(orderBy, dir).
			Limit(maxBatchSize)
		if lastDocumentId != "" {
			c.logger.Info().Msgf("Starting after %s", lastDocumentId)
			query = query.StartAfter(lastDocumentId)
		}
		docIter := query.Documents(ctx)
		var documentCount int
		var skippedCount int
		for {
			docSnap, err := docIter.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				return err
			}
			documentCount++
			if !docSnap.Exists() {
				skippedCount++
				continue
			}
			lastDocumentId = docSnap.Ref.ID
			item := docSnap.Data()
			subCollectionIter := docSnap.Ref.Collections(ctx)
			resource := schema.NewResourceData(table, nil, nil)
			pushData := func() error {
				if c.nestedCollections {
					for {
						subCollection, err := subCollectionIter.Next()
						if err != nil {
							if err == iterator.Done {
								break
							}
							return err
						}

						subCollectionName := collection.ID + "_" + subCollection.ID
						// find the table
						var subCollectionTable *schema.Table
						for _, t := range c.Tables {
							if t.Name == subCollectionName {
								subCollectionTable = t
								break
							}
						}

						subCollectionItems := make([]interface{}, 0)
						subCollectionDocIter := subCollection.Query.Documents(ctx)
						for {
							subCollectionDocSnap, err := subCollectionDocIter.Next()
							if err != nil {
								if err == iterator.Done {
									break
								}
								return err
							}
							subCollectionItem := subCollectionDocSnap.Data()
							if c.nestedCollectionsTables {
								if subCollectionTable != nil {
									subResource := schema.NewResourceData(subCollectionTable, resource, subCollectionItems)
									subResource.Set("__id", subCollectionDocSnap.Ref.ID)
									subResource.Set("__parent_id", docSnap.Ref.ID)
									subResource.Set("__created_at", docSnap.CreateTime)
									subResource.Set("__updated_at", docSnap.UpdateTime)
									subResource.Set("data", subCollectionItem)
									c.metrics.TableClient[table.Name][c.ID()].Resources++
									res <- subResource
								} else {
									c.logger.Warn().Msgf("Table %s not found", subCollectionName)
								}
							} else {
								item[subCollectionName] = subCollectionItems
							}
						}
					}
				}
				err = resource.Set("__id", docSnap.Ref.ID)
				if err != nil {
					return err
				}
				err = resource.Set("__created_at", docSnap.CreateTime)
				if err != nil {
					return err
				}
				err = resource.Set("__updated_at", docSnap.UpdateTime)
				if err != nil {
					return err
				}
				err = resource.Set("data", item)
				if err != nil {
					return err
				}
				c.metrics.TableClient[table.Name][c.ID()].Resources++
				res <- resource
				return nil
			}
			if c.nestedCollections {
				eg.Go(pushData)
			} else {
				pushData()
			}
		}
		c.logger.Info().Msgf("Synced %d documents from %s", documentCount, table.Name)
		if skippedCount > 0 {
			c.logger.Info().Msgf("Skipped %d documents from %s", skippedCount, table.Name)
		}
		if documentCount < maxBatchSize {
			break
		}
	}
	return eg.Wait()
}

func (c *Client) syncTables(ctx context.Context, res chan<- *schema.Resource) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(len(c.Tables))

	for _, table := range c.Tables {
		t := table
		eg.Go(func() error {
			return c.syncTable(ctx, t, res)
		})
	}
	return eg.Wait()
}
