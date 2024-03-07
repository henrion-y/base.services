package repository

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"gorm.io/gorm"
)

/*
将查询都分成 匹配、 排序、 截取(翻页) 三步， 再根据这三步逐步扩展能力
*/

type BaseRepository interface {
	Create(ctx context.Context, mod Model) error
	Update(ctx context.Context, mod Model, data map[string]interface{}, filterGroup *FilterGroup) error
	Delete(ctx context.Context, mod Model, filterGroup *FilterGroup) error
	Find(ctx context.Context, mod Model, result interface{}, fields []string, filterGroup *FilterGroup, sortSpecs *SortSpecs, limitSpec *LimitSpec) error
	FindOne(ctx context.Context, mod Model, fields []string, filterGroup *FilterGroup, sortSpecs *SortSpecs) error
	Count(ctx context.Context, mod Model, filterGroup *FilterGroup) (int64, error)
}

type Model interface {
	TableName() string
}

/********* 匹配 ***********/

type FilterType string

const (
	FilterType_EQ     FilterType = "EQ"     //相等
	FilterType_NE     FilterType = "NE"     //不相等
	FilterType_GT     FilterType = "GT"     //大于
	FilterType_GTE    FilterType = "GTE"    // 大于等于
	FilterType_LT     FilterType = "LT"     //小于
	FilterType_LTE    FilterType = "LTE"    //小于等于
	FilterType_IN     FilterType = "IN"     //在什么范围内
	FilterType_NOT_IN FilterType = "NOT_IN" //不在什么范围内
	FilterType_LIKE   FilterType = "LIKE"   //like

)

type FilterLogic string

const (
	FilterLogic_OR  FilterLogic = "OR"
	FilterLogic_AND FilterLogic = "AND"
)

type FilterSpec struct {
	Column     string
	Value      interface{}
	FilterType FilterType
}

type FilterGroup struct {
	Filters []FilterSpec
	Logic   FilterLogic
	Groups  []*FilterGroup
}

// NewFilterGroup 创建一个新的过滤组
func NewFilterGroup() *FilterGroup {
	return &FilterGroup{}
}

// Equals 添加一个相等的过滤条件
func (g *FilterGroup) Equals(column string, value interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_EQ, value)
}

// NotEquals 添加一个不相等的过滤条件
func (g *FilterGroup) NotEquals(column string, value interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_NE, value)
}

// GreaterThan 添加一个大于的过滤条件
func (g *FilterGroup) GreaterThan(column string, value interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_GT, value)
}

// GreaterThanOrEqual 添加一个大于等于的过滤条件
func (g *FilterGroup) GreaterThanOrEqual(column string, value interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_GTE, value)
}

// LessThan 添加一个小于的过滤条件
func (g *FilterGroup) LessThan(column string, value interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_LT, value)
}

// LessThanOrEqual 添加一个小于等于的过滤条件
func (g *FilterGroup) LessThanOrEqual(column string, value interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_LTE, value)
}

// In 添加一个在指定范围内的过滤条件
func (g *FilterGroup) In(column string, values interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_IN, values)
}

// NotIn 添加一个不在指定范围内的过滤条件
func (g *FilterGroup) NotIn(column string, values interface{}) *FilterGroup {
	return g.AddFilter(column, FilterType_NOT_IN, values)
}

// Like 添加一个LIKE的过滤条件
func (g *FilterGroup) Like(column string, pattern string) *FilterGroup {
	return g.AddFilter(column, FilterType_LIKE, pattern)
}

// AddFilter 是一个通用的方法，用于将过滤器添加到组中
func (g *FilterGroup) AddFilter(column string, filterType FilterType, value interface{}) *FilterGroup {
	g.Filters = append(g.Filters, FilterSpec{
		Column:     column,
		FilterType: filterType,
		Value:      value,
	})
	return g
}

// AddGroup 添加一个新的子组到当前的过滤组中  Struct FilterGroup has methods on both value and pointer receivers. Such usage is not recommended by the Go Documentation.
func (g *FilterGroup) AddGroup(subGroup *FilterGroup) *FilterGroup {
	g.Groups = append(g.Groups, subGroup)
	return g
}

// SetLogic 设置组的逻辑关系
func (g *FilterGroup) SetLogic(logic FilterLogic) *FilterGroup {
	g.Logic = logic
	return g
}

// And adds a set of filters and groups with AND logic.
func (g *FilterGroup) And(filters ...*FilterGroup) *FilterGroup {
	newGroup := &FilterGroup{Logic: FilterLogic_AND}
	for _, f := range filters {
		if f != nil {
			newGroup.Groups = append(newGroup.Groups, f)
		}
	}
	return g.AddGroup(newGroup)
}

// Or adds a set of filters and groups with OR logic.
func (g *FilterGroup) Or(filters ...*FilterGroup) *FilterGroup {
	newGroup := &FilterGroup{Logic: FilterLogic_OR}
	for _, f := range filters {
		if f != nil {
			newGroup.Groups = append(newGroup.Groups, f)
		}
	}
	return g.AddGroup(newGroup)
}

func (g *FilterGroup) BuildToMysql(db *gorm.DB) *gorm.DB {
	// 应用这一层的过滤条件
	for _, filter := range g.Filters {
		// 根据比较类型生成查询表达式
		expression := fmt.Sprintf("%s %s ?", filter.Column, toMySQLComparator(filter.FilterType))
		db = db.Where(expression, filter.Value)
	}

	// 如果有嵌套的FilterGroup，根据当前组的Logic来递归构建查询条件
	if len(g.Groups) > 0 {
		subQuery := db
		if g.Logic == FilterLogic_OR {
			// 为嵌套 OR 查询创建一个新的子查询
			subQuery = db.Session(&gorm.Session{NewDB: true})
		}

		for _, subGroup := range g.Groups {
			if g.Logic == FilterLogic_OR {
				// 使用`Or`链接子组条件，需要先构建完整的嵌套条件
				// 然后再将结果作为OR条件应用到原始查询
				nested := subGroup.BuildToMysql(subQuery)
				db = db.Or(nested)
			} else {
				// 默认按照AND链接子组条件使用现有的db实例
				db = subGroup.BuildToMysql(db)
			}
		}
	}

	return db
}

func (g *FilterGroup) BuildToMongo() bson.D {
	var topLevelConditions bson.A

	// 处理 g.Filters 中的顶层过滤器
	for _, filter := range g.Filters {
		operator := "$eq" // 这是默认的比较操作符
		// 此处省略了你之前的逻辑，根据具体的filter.Operator设置不同的MongoDB操作符
		switch filter.FilterType {
		case FilterType_NE:
			operator = "$ne"
		case FilterType_GT:
			operator = "$gt"
		case FilterType_GTE:
			operator = "$gte"
		case FilterType_LT:
			operator = "$lt"
		case FilterType_LTE:
			operator = "$lte"
		case FilterType_IN:
			operator = "$in"
		case FilterType_NOT_IN:
			operator = "$nin"
		case FilterType_LIKE:
			// MongoDB使用正则表达式来实现LIKE功能
			operator = "$regex"
			filter.Value = primitive.Regex{Pattern: filter.Value.(string), Options: "i"} // 我们假设filter.Value是一个字符串
		}
		topLevelConditions = append(topLevelConditions, bson.D{{Key: filter.Column, Value: bson.D{{Key: operator, Value: filter.Value}}}})
	}

	// 处理子过滤器组 g.Groups
	for _, subgroup := range g.Groups {
		subFilterDoc := subgroup.BuildToMongo() // 递归构建子过滤器的查询条件
		// 这里检查子过滤器是否为空，如果为空则跳过
		if len(subFilterDoc) == 0 {
			continue
		}
		// 根据子过滤器组的逻辑，添加一个适当的逻辑操作符和子过滤器条件的数组
		logicOperator := "$and" // 默认使用 $and
		if subgroup.Logic == FilterLogic_OR {
			logicOperator = "$or" // 如果逻辑是 OR，则改为 $or
		}
		topLevelConditions = append(topLevelConditions, bson.D{{Key: logicOperator, Value: bson.A{subFilterDoc}}})
	}

	if len(topLevelConditions) == 0 {
		// 如果没有任何条件，返回一个空的 bson.D
		return bson.D{}
	}

	// 使用顶层的逻辑连接条件，如 $and 或 $or
	// 注意，这里假设了顶层逻辑不为空
	logicOperator := "$and"
	if g.Logic == FilterLogic_OR {
		logicOperator = "$or"
	}
	return bson.D{{Key: logicOperator, Value: topLevelConditions}}
}

func toMySQLComparator(filterType FilterType) string {
	switch filterType {
	case FilterType_EQ:
		return "="
	case FilterType_NE:
		return "<>"
	case FilterType_GT:
		return ">"
	case FilterType_GTE:
		return ">="
	case FilterType_LT:
		return "<"
	case FilterType_LTE:
		return "<="
	case FilterType_LIKE:
		return "LIKE"
	// IN 和 NOT IN 需要特殊处理，因为它们涉及到切片作为参数
	case FilterType_IN:
		return "IN"
	case FilterType_NOT_IN:
		return "NOT IN"
	default:
		panic("unsupported filter type")
	}
}

/********* 排序 ***********/

type SortType string

const (
	SortType_ASC  SortType = "ASC"  // 升序
	SortType_DESC SortType = "DESC" // 降序
)

type SortSpec struct {
	Property string   `json:"property"` // 属性名
	Type     SortType `json:"type"`     // 排序类型
}

type SortSpecs []SortSpec

func NewSortSpecs(property string, sortType SortType) *SortSpecs {
	return &SortSpecs{{property, sortType}}
}

func NewDefaultSortSpecs() *SortSpecs {
	return &SortSpecs{}
}

func (s *SortSpecs) Add(property string, sortType SortType) *SortSpecs {
	*s = append(*s, SortSpec{property, sortType})
	return s
}

func (s *SortSpecs) AddDesc(property string) *SortSpecs {
	*s = append(*s, SortSpec{property, SortType_DESC})
	return s
}

func (s *SortSpecs) AddAsc(property string) *SortSpecs {
	*s = append(*s, SortSpec{property, SortType_ASC})
	return s
}

var mongoSortTypeSet = map[SortType]int{
	SortType_ASC:  1,
	SortType_DESC: -1,
}

func (s *SortSpecs) BuildToMongo() bson.D {
	sortSpecs := bson.D{}
	for i := range *s {
		sortSpecs = append(sortSpecs, bson.E{Key: (*s)[i].Property, Value: mongoSortTypeSet[(*s)[i].Type]})
	}
	return sortSpecs
}

var mysqlSortTypeSet = map[SortType]string{
	SortType_ASC:  "ASC",
	SortType_DESC: "DESC",
}

func (s *SortSpecs) BuildToMysql(gormDb *gorm.DB) {
	for i := range *s {
		gormDb.Order(fmt.Sprintf("%s %s", (*s)[i].Property, mysqlSortTypeSet[(*s)[i].Type]))
	}
}

/********* 翻页 ***********/

type LimitSpec struct {
	Page int
	Size int
}

func NewLimitSpec(page int, size int) *LimitSpec {
	return &LimitSpec{
		page,
		size,
	}
}

func (s *LimitSpec) BuildToMysql(gormDb *gorm.DB) {
	if s.Size > 0 {
		gormDb.Limit(s.Size)
	}
	if s.Page > 1 {
		gormDb.Offset((s.Page - 1) * s.Size)
	}
}

func (s *LimitSpec) BuildToMongo() (optLimit *int64, optSkip *int64) {
	if s.Size > 0 {
		limit := int64(s.Size)
		optLimit = &limit
	}
	if s.Page > 1 {
		skip := int64((s.Page - 1) * s.Size)
		optSkip = &skip
	}
	return
}
