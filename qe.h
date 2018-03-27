#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <cmath>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

class AggregateHandle;
// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};

struct Condition {
    std::string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(std::vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                std::string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
            delete iter;
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(std::vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                std::string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
            delete iter;
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter();

        RC getNextTuple(void *data);
        // For attribute in std::vector<Attribute>, name it as rel.attr
        void getAttributes(std::vector<Attribute> &attrs) const;

    private:
        Iterator* iter_input_;
        Condition condition_;
        std::vector<Attribute> attrs_;
};

class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const std::vector<std::string> &attrNames);   // std::vector containing attribute names
        ~Project();

        RC getNextTuple(void *data);
        // For attribute in std::vector<Attribute>, name it as rel.attr
        void getAttributes(std::vector<Attribute> &attrs) const;

    private:
        Iterator* iter_input_;
        std::vector<std::string> projected_attrnames_;
        std::vector<Attribute> input_attrs_;
        std::vector<Attribute> project_attrs_;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin();

        RC getNextTuple(void *data);
        // For attribute in std::vector<Attribute>, name it as rel.attr
        void getAttributes(std::vector<Attribute> &attrs) const;

        bool LoadBlock();

        bool GetOutTupleViaBlock(void* data, unsigned& data_length);
    private:
        // std::string tableName;
        Iterator *outer_;
        TableScan *inner_;

        std::vector<Attribute> out_attrs_;
        std::vector<Attribute> in_attrs_;

        Condition condition_;
        AttrType condition_attr_type_;
        unsigned out_condition_attrIndex_;
        unsigned in_condition_attr_index_;

        unsigned buffer_size_;
        char* block_data_;
        std::vector<unsigned> data_length_map_; // record_length = data_length_map_[cur_pos], record_count = data_length_map_.size()
        unsigned max_out_recrod_length_;
        unsigned cur_pos_in_block_; // cur_offset = max_out_recrod_length_ * cur_pos

        // std::map<KeyType, std::vector<std::Pair> > block_mapper_;
        // unsigned block_free_lenghth_;


        // unsigned current_proper_vector_index_;
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin();

        RC getNextTuple(void *data);
        // For attribute in std::vector<Attribute>, name it as rel.attr
        void getAttributes(std::vector<Attribute> &attrs) const;
    private:
        Iterator *outer_;
        IndexScan *inner_;

        std::vector<Attribute> out_attrs_;
        std::vector<Attribute> in_attrs_;

        Condition condition_;
        AttrType condition_attr_type_;
        unsigned out_condition_attrIndex_;
        unsigned in_condition_attr_index_;

        // unsigned buffer_size_;
        // char* block_data_;
        // std::vector<unsigned> data_length_map_; // record_length = data_length_map_[cur_pos], record_count = data_length_map_.size()
        // unsigned max_out_recrod_length_;
        // unsigned cur_pos_in_block_; // cur_offset = max_out_recrod_length_ * cur_pos

};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      );
      ~GHJoin();

      RC getNextTuple(void *data);
      // For attribute in std::vector<Attribute>, name it as rel.attr
      void getAttributes(std::vector<Attribute> &attrs) const;
};

class AggregateHandle{
public:
    AggregateHandle(const Attribute& agg_attr, const AggregateOp& agg_op);
    // Init();

    void Append(float append_value); // switch (this->op_)

    bool Result(void* data);
private:
    float float_value_;
    // int int_value_;
    unsigned count_;

    // ??
    Attribute agg_attr_;
    AggregateOp agg_op_;
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );
        ~Aggregate();

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(std::vector<Attribute> &attrs) const;
    private:
        AggregateHandle* aggregate_handle_;
        Iterator *input_;
        std::vector<Attribute> attrs_;
        Attribute agg_attr_;
        AggregateOp agg_op_;

        bool is_group_;
};

class TupleHelper{
public:
    static RC JoinData(const char* out_data_tmp, const std::vector<Attribute>& out_attrs, const char* in_data_tmp, const std::vector<Attribute>& in_attrs, char* data);

	// Retrieve specified attribute from formated record returned by GetNextRecord/GetNextTuple
	// which would not include the record header that stores attributes' offset
	static RC RetrieveMatchedAttribute(const void* record_data, const std::vector<Attribute>& attrs,
								const std::string attr_name, Value& matched_value, unsigned& attr_len){
	unsigned offset = 0;

	// WARNING: add nullbits
	unsigned num_attrs = attrs.size();
	unsigned null_bits_len = 0;
	std::vector<bool> null_bool_vec = CheckNullbits(num_attrs, (char*)record_data + offset, null_bits_len);
	offset += null_bits_len;
    for (size_t i = 0; i < attrs.size(); ++i) {
    	if(!null_bool_vec[i]){ // false means it's not null
            if (attrs[i].type != TypeVarChar) {
                attr_len = kFourBtyesSpace;
                if (attrs[i].name == attr_name) {
                    // get left value
	                matched_value.data = realloc(matched_value.data, kFourBtyesSpace);
                    memcpy((char *)matched_value.data, (char *)record_data + offset, kFourBtyesSpace);
                    matched_value.type = attrs[i].type;
                    return 0;
                }
                offset += attr_len;
            }
            else {
                attr_len = 0;
                memcpy(&attr_len, (char *)record_data + offset, kFourBtyesSpace);
                attr_len += kFourBtyesSpace;
                if (attrs[i].name == attr_name) {
                    matched_value.data = realloc(matched_value.data, attr_len);
                    memcpy((char *)matched_value.data, (char *)record_data + offset, attr_len);
                    matched_value.type = attrs[i].type;
                    return 0;
                }
                offset += attr_len;
            }

	    }
	    else {
#ifndef NO_CERR
	    	std::cerr << "it's werid to have none zero null bit";
#endif
	    }
    }

    return -1;
	}

	// IsEqual(); like IsComp in rbf, check is two data's value is equal
	static bool IsComped(const Value& lhs_value, const Value& rhs_value, const Condition& condition){
		// check if lhs and rhs is the same type:
		if(condition.op == NO_OP){
			return true;
		}

		if(lhs_value.type != rhs_value.type)
			return false;

		//  check is same value
		switch(lhs_value.type){
			case TypeVarChar:
			{
				std::string lhs_string = RetrieveString(lhs_value.data);
				std::string rhs_string = RetrieveString(rhs_value.data);
	            return Compare<std::string>(lhs_string, rhs_string, condition.op); // dont mess up left and right
				break;
			}
			case TypeInt:
			{
				int lhs_int, rhs_int;
				memcpy(&lhs_int, lhs_value.data, kFourBtyesSpace);
				memcpy(&rhs_int, rhs_value.data, kFourBtyesSpace);
				return Compare<int>(lhs_int, rhs_int, condition.op);
				break;
			}
			case TypeReal:
			{
				float lhs_f, rhs_f;
				memcpy(&lhs_f, lhs_value.data, kFourBtyesSpace);
				memcpy(&rhs_f, rhs_value.data, kFourBtyesSpace);
				return Compare<int>(lhs_f, rhs_f, condition.op);
				break;
			}
	        default:
            	break;
		};// end switch
		return false; // error
	}

	static std::string RetrieveString(const void* varchar_data){
		unsigned str_len;
		memcpy(&str_len, varchar_data, kFourBtyesSpace);
		return std::string((char *)varchar_data + kFourBtyesSpace, str_len);
	}

	static unsigned RetrieveLengthOfAttr(const void* data, const Attribute& attr){
		switch(attr.type){
			case TypeVarChar:
			{
				unsigned str_len;
				memcpy(&str_len, data, kFourBtyesSpace);
				return (unsigned)str_len + kFourBtyesSpace;
			}
			case TypeInt:
			{
				return kFourBtyesSpace;
			}
			case TypeReal:
			{
				return kFourBtyesSpace;
			}
	        default:
            	break;
		};
		return false; // error
	}

	static std::vector<bool> CheckNullbits(const unsigned num_attrs, const char* nullbits, unsigned& null_bits_len){
		std::vector<bool> return_null_bits_vec;
		return_null_bits_vec.resize((unsigned)num_attrs);
		null_bits_len = (unsigned)std::ceil((float)num_attrs / 8.0);
		for (size_t i = 0; i < null_bits_len; i++){
			char eight_bits = *(nullbits + i);
			for(size_t j = 0; j < 8; j++) {
				if((8 * i + j) < num_attrs) {
					char bit = ((1 << (7-j)) & eight_bits);
					return_null_bits_vec[8 * i + j] = (bit == 0? false: true);// false means it's not null
				}
				else
					break;
			}
		}
		return return_null_bits_vec;
	}

	static void WriteNullbits(const std::vector<bool> null_bits_vec, char* data){ // unsigned& num_attrs
		unsigned num_attrs = null_bits_vec.size();
		unsigned null_bits_len = (unsigned)std::ceil((float)num_attrs / 8.0);
		for (size_t i = 0; i < null_bits_len; i++){
			char eight_bits = 0;
			for(size_t j = 0; j < 8; j++){
				if((8 * i + j) < num_attrs){
					if(null_bits_vec[8 * i + j] == true) // false means a null bit set to 1
						eight_bits |= (1 << (7-j));
				}
				else
					continue;
			}
			*(data + i) = eight_bits;
		}
		// null_bits_offset = null_bits_len;
		return ;
	}

};

#endif
