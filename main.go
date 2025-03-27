package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	helmet "github.com/danielkov/gin-helmet"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"
)

type ListModel struct {
	Id          sql.NullInt64
	MlId        sql.NullString
	MerchantId  sql.NullString
	Name        sql.NullString
	LongDesc    sql.NullString
	ShortDesc   sql.NullString
	Icon        sql.NullString
	Quota       sql.NullString
	StartPeriod sql.NullString
	EndPeriod   sql.NullString
}

type ListEntity struct {
	Id          int     `json:"id"`
	MlId        *string `json:"mlId"`
	MerchantId  *string `json:"merchantId"`
	Name        *string `json:"name"`
	LongDesc    *string `json:"longDesc"`
	ShortDesc   *string `json:"shortDesc"`
	Icon        *string `json:"icon"`
	Quota       *string `json:"quota"`
	StartPeriod *string `json:"startPeriod"`
	EndPeriod   *string `json:"endPeriod"`
}

type Params struct {
	Page  int
	Limit int
}

func main() {
	ctx := context.Background()
	db, err := connectDatabase()

	if err != nil {
		panic(err)
	}

	var productType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Product",
		Fields: graphql.Fields{
			"id": &graphql.Field{Type: graphql.Int},
			"mlId": &graphql.Field{
				Type: graphql.String,
			},
			"merchantId": &graphql.Field{
				Type: graphql.String,
			},
			"name": &graphql.Field{
				Type: graphql.String,
			},
			"longDesc": &graphql.Field{
				Type: graphql.String,
			},
			"shortDesc": &graphql.Field{
				Type: graphql.String,
			},
			"icon": &graphql.Field{
				Type: graphql.String,
			},
			"quota": &graphql.Field{
				Type: graphql.String,
			},
			"startPeriod": &graphql.Field{
				Type: graphql.String,
			},
			"endPeriod": &graphql.Field{
				Type: graphql.String,
			},
		},
	})

	var productPaginationType = graphql.NewObject(graphql.ObjectConfig{
		Name: "ProductPagination",
		Fields: graphql.Fields{
			"page":       &graphql.Field{Type: graphql.Int},
			"limit":      &graphql.Field{Type: graphql.Int},
			"totalData":  &graphql.Field{Type: graphql.Int},
			"totalPages": &graphql.Field{Type: graphql.Int},
			"data":       &graphql.Field{Type: graphql.NewList(productType)},
		},
	})

	var rootQuery = graphql.NewObject(graphql.ObjectConfig{
		Name: "RootQuery",
		Fields: graphql.Fields{
			"products": &graphql.Field{
				Type: productPaginationType,
				Args: graphql.FieldConfigArgument{
					"page":  &graphql.ArgumentConfig{Type: graphql.Int},
					"limit": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					limit := 10
					page := 1

					if val, ok := p.Args["limit"].(int); ok {
						limit = val
					}
					if val, ok := p.Args["page"].(int); ok && val > 1 {
						page = val
					}

					total, err := fetchTotalData(db, ctx)
					if err != nil {
						return nil, err
					}

					d := float64(total) / float64(limit)
					totalPages := int(math.Ceil(d))

					params := Params{
						Page:  page,
						Limit: limit,
					}

					list, err := fetchList(db, ctx, params)
					if err != nil {
						return nil, err
					}
					return map[string]interface{}{
						"data":       list,
						"page":       page,
						"limit":      limit,
						"totalData":  int(total),
						"totalPages": totalPages,
					}, nil
				},
			},
			"product": &graphql.Field{
				Type: productType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{
						Type: graphql.Int,
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					id, ok := p.Args["id"].(int)
					if ok {
						data, err := fetchOne(db, ctx, id)
						if err != nil {
							return nil, err
						}
						return data, nil
					}
					return nil, nil
				},
			},
		},
	})

	var rootMutation = graphql.NewObject(graphql.ObjectConfig{
		Name: "RootMutation",
		Fields: graphql.Fields{
			"createProduct": &graphql.Field{
				Type: productType,
				Args: graphql.FieldConfigArgument{
					"mlId": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"merchantId": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"name": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"longDesc": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"shortDesc": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"icon": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"quota": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"startPeriod": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"endPeriod": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					mlId, _ := p.Args["mlId"].(string)
					merchantId, _ := p.Args["merchantId"].(string)
					name, _ := p.Args["name"].(string)
					longDesc, _ := p.Args["longDesc"].(string)
					shortDesc, _ := p.Args["shortDesc"].(string)
					icon, _ := p.Args["icon"].(string)
					quota, _ := p.Args["quota"].(string)
					startPeriod, _ := p.Args["startPeriod"].(string)
					endPeriod, _ := p.Args["endPeriod"].(string)

					input := &ListModel{
						MlId:        sql.NullString{String: mlId, Valid: true},
						MerchantId:  sql.NullString{String: merchantId, Valid: true},
						Name:        sql.NullString{String: name, Valid: true},
						LongDesc:    sql.NullString{String: longDesc, Valid: true},
						ShortDesc:   sql.NullString{String: shortDesc, Valid: true},
						Icon:        sql.NullString{String: icon, Valid: true},
						Quota:       sql.NullString{String: quota, Valid: true},
						StartPeriod: sql.NullString{String: startPeriod, Valid: true},
						EndPeriod:   sql.NullString{String: endPeriod, Valid: true},
					}

					data, err := createProduct(db, ctx, input)
					if err != nil {
						return nil, err
					}
					return data, nil
				},
			},
		},
	})

	var schema, _ = graphql.NewSchema(graphql.SchemaConfig{
		Query:    rootQuery,
		Mutation: rootMutation,
	})

	// setup router
	router := gin.Default()

	// Set a lower memory limit for multipart forms (default is 32 MiB)
	router.MaxMultipartMemory = 10 << 20 // 10 MiB

	// Setup Mode Application
	if os.Getenv("APP_ENV") == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	// setup cors origin
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowHeaders:     []string{"*"},
		AllowMethods:     []string{"*"},
		AllowCredentials: true,
		AllowWildcard:    true,
		ExposeHeaders:    []string{"Content-Length"},
	}))
	router.Use(helmet.Default())
	router.Use(gzip.Gzip(gzip.BestCompression))

	router.POST("/graphql", func(c *gin.Context) {
		var params struct {
			Query string `json:"query"`
		}

		if err := c.ShouldBindJSON(&params); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		result := graphql.Do(graphql.Params{
			Schema:        schema,
			RequestString: params.Query,
		})

		c.JSON(http.StatusOK, result)
	})

	// serve http
	log.Fatal(router.Run(":" + os.Getenv("APP_PORT")))
}

func connectDatabase() (*sql.DB, error) {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		return nil, err
	}

	conn := mysql.Config{
		User:                 os.Getenv("DB_USER"),
		Passwd:               os.Getenv("DB_PASS"),
		DBName:               "wec_product",
		Addr:                 fmt.Sprintf("%s:%s", os.Getenv("DB_HOST"), os.Getenv("DB_PORT")),
		Net:                  "tcp",
		ParseTime:            true,
		Loc:                  loc,
		AllowNativePasswords: true,
		Timeout:              60 * time.Second,
	}

	dsn := conn.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(50)

	return db, nil
}

func fetchList(db *sql.DB, ctx context.Context, params Params) ([]*ListEntity, error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	offset := (params.Page - 1) * params.Limit
	query := "SELECT id, ml_id, merchant_id, name, long_desc, short_desc, icon, quota, start_period, end_period from products p limit ? offset ?"

	var listModel []*ListModel
	var list []*ListEntity
	stmt, err := db.Prepare(query)
	if err != nil {
		return list, err
	}

	rows, err := stmt.QueryContext(ctx, params.Limit, offset)
	if err != nil {
		return list, err
	}

	if rows.Err() != nil {
		return list, rows.Err()
	}

	for rows.Next() {
		var data ListModel
		err = rows.Scan(
			&data.Id,
			&data.MlId,
			&data.MerchantId,
			&data.Name,
			&data.LongDesc,
			&data.ShortDesc,
			&data.Icon,
			&data.Quota,
			&data.StartPeriod,
			&data.EndPeriod,
		)

		if err != nil {
			break
		}

		listModel = append(listModel, &data)
	}

	if err != nil {
		return list, err
	}
	defer rows.Close()

	for _, item := range listModel {
		list = append(list, &ListEntity{
			Id:          int(item.Id.Int64),
			MlId:        &item.MlId.String,
			MerchantId:  &item.MerchantId.String,
			Name:        &item.Name.String,
			LongDesc:    &item.LongDesc.String,
			ShortDesc:   &item.ShortDesc.String,
			Icon:        &item.Icon.String,
			Quota:       &item.Quota.String,
			StartPeriod: &item.StartPeriod.String,
			EndPeriod:   &item.EndPeriod.String,
		})
	}

	fmt.Println("waktu mulai :", now.Format("2006-01-02 15:04:05"), "waktu selesai:", time.Now().Format("2006-01-02 15:04:05"))
	return list, nil
}

func fetchTotalData(db *sql.DB, ctx context.Context) (int64, error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := "SELECT count(id) from products"

	var totalData int64
	stmt, err := db.Prepare(query)
	if err != nil {
		return totalData, err
	}

	err = stmt.QueryRowContext(ctx).Scan(&totalData)

	if err != nil {
		return totalData, err
	}
	fmt.Println("Total :", totalData)
	fmt.Println("waktu mulai :", now.Format("2006-01-02 15:04:05"), "waktu selesai:", time.Now().Format("2006-01-02 15:04:05"))
	return totalData, nil
}

func fetchOne(db *sql.DB, ctx context.Context, id int) (*ListEntity, error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := "SELECT id, ml_id, merchant_id, name, long_desc, short_desc, icon, quota, start_period, end_period from products p where p.id = ? limit 1"

	var data ListModel

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	row := stmt.QueryRowContext(ctx, id)
	err = row.Scan(
		&data.Id,
		&data.MlId,
		&data.MerchantId,
		&data.Name,
		&data.LongDesc,
		&data.ShortDesc,
		&data.Icon,
		&data.Quota,
		&data.StartPeriod,
		&data.EndPeriod,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}

		return nil, err
	}
	defer stmt.Close()

	one := &ListEntity{
		Id:          int(data.Id.Int64),
		MlId:        &data.MlId.String,
		MerchantId:  &data.MerchantId.String,
		Name:        &data.Name.String,
		LongDesc:    &data.LongDesc.String,
		ShortDesc:   &data.ShortDesc.String,
		Icon:        &data.Icon.String,
		Quota:       &data.Quota.String,
		StartPeriod: &data.StartPeriod.String,
		EndPeriod:   &data.EndPeriod.String,
	}

	fmt.Println("waktu mulai :", now.Format("2006-01-02 15:04:05"), "waktu selesai:", time.Now().Format("2006-01-02 15:04:05"))
	return one, nil
}

func createProduct(db *sql.DB, ctx context.Context, input *ListModel) (*ListEntity, error) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := "INSERT INTO products (ml_id, merchant_id, name, long_desc, short_desc, icon, quota, start_period, end_period) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	res, err := stmt.ExecContext(ctx,
		input.MlId.String,
		input.MerchantId.String,
		input.Name.String,
		input.LongDesc.String,
		input.ShortDesc.String,
		input.Icon.String,
		input.Quota.String,
		input.StartPeriod.String,
		input.EndPeriod.String,
	)

	if err != nil {
		return nil, err
	}

	lastId, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	one, err := fetchOne(db, ctx, int(lastId))
	if err != nil {
		return nil, err
	}

	fmt.Println("waktu mulai :", now.Format("2006-01-02 15:04:05"), "waktu selesai:", time.Now().Format("2006-01-02 15:04:05"))
	return one, nil
}
