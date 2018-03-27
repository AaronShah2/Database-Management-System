#include <iostream>
#include <algorithm>
#include <cmath>
#include <memory>
#include <string.h>
#include <limits>

#include "qe.h"

// ------------- Filter operator ------------- 
Filter::Filter(Iterator* input, const Condition &condition)
	:iter_input_(input), condition_(condition){
	this->iter_input_->getAttributes(this->attrs_);
}

// TO DELETE
Filter::~Filter(){
}

RC Filter::getNextTuple(void *data) {
	while(this->iter_input_->getNextTuple(data) != QE_EOF) {
		if(this->condition_.bRhsIsAttr) {
			Value lhs_value, rhs_value;
			lhs_value.data = malloc(kFourBtyesSpace); // will be realloc
			rhs_value.data = malloc(kFourBtyesSpace); // will be realloc
			
			unsigned len_lhs_value, len_rhs_value;
			TupleHelper::RetrieveMatchedAttribute(data, this->attrs_, this->condition_.lhsAttr, lhs_value, len_lhs_value);
			TupleHelper::RetrieveMatchedAttribute(data, this->attrs_, this->condition_.rhsAttr, rhs_value, len_rhs_value);
			bool is_comped = TupleHelper::IsComped(lhs_value, rhs_value, this->condition_);
			free(lhs_value.data);
			free(rhs_value.data);

			if(is_comped)
				return 0;
			else
				continue;
		}
		else {
			Value lhs_value;
			lhs_value.data = malloc(kFourBtyesSpace); // will be realloc
			unsigned len_lhs_value;
			TupleHelper::RetrieveMatchedAttribute(data, this->attrs_, this->condition_.lhsAttr, lhs_value, len_lhs_value);
			bool is_comped = TupleHelper::IsComped(lhs_value, this->condition_.rhsValue, this->condition_);
			free(lhs_value.data);
			if(is_comped)
				return 0;
			else
				continue;
		}
	}

	return QE_EOF;
}

// For attribute in std::vector<Attribute>, name it as rel.attr
void Filter::getAttributes(std::vector<Attribute> &attrs) const{
	this->iter_input_->getAttributes(attrs);
}

// ------------- Projection operator -------------

Project::Project(Iterator *input, const std::vector<std::string> &attrNames)
				:iter_input_(input), projected_attrnames_(attrNames){
	this->iter_input_->getAttributes(this->input_attrs_);

	for(auto& p_name : projected_attrnames_){
		for (auto& attr : this->input_attrs_){
			if (p_name == attr.name){
				// Attribute attr;
				// attr.name = attrs_[j].name;
				// attr.type = attrs_[j].type;
				// attr.length = attrs_[j].length;
				this->project_attrs_.push_back(attr);
			}
		}
	}
}   // std::vector containing attribute names

// TO DELETE
Project::~Project(){

}

RC Project::getNextTuple(void *data) {
	void* tuple_buf = malloc(4096);
	if(this->iter_input_->getNextTuple(tuple_buf) != QE_EOF) {
		unsigned data_offset = 0;
		data_offset += (unsigned)std::ceil((float)projected_attrnames_.size() / 8.0);

		unsigned input_num_attrs = input_attrs_.size();
		unsigned input_null_bits_len = 0;
		std::vector<bool> input_null_bool_vec = TupleHelper::CheckNullbits(input_num_attrs, (char*)tuple_buf, input_null_bits_len);
		// offset += null_bits_len;

		std::vector<bool> projected_null_vec(projected_attrnames_.size(), false);
		// WARNING: add nullbits for return data too.
		for(size_t i = 0; i < this->projected_attrnames_.size(); i++){
			unsigned buf_offset = input_null_bits_len;
			// WARNING: add nullbits
			for(size_t j = 0; j < this->input_attrs_.size(); j++){
				unsigned attr_len = TupleHelper::RetrieveLengthOfAttr(tuple_buf + buf_offset, input_attrs_[j]);
				if(input_attrs_[j].name == projected_attrnames_[i]){
					if(!input_null_bool_vec[j]) {
		                memcpy((char *)data + data_offset, (char *)tuple_buf + buf_offset, attr_len);
		                data_offset += attr_len;
						projected_null_vec[i] = false;
		                // WARNING: set nullbits if null
		                break;
		            }
					else {
						projected_null_vec[i] = true;
						break;
					}
				} // else do not copy attr;
				buf_offset += attr_len;
				if(j == input_attrs_.size() - 1) { // still not jump out
					projected_null_vec[i] = true;
				}
			}
		}
		TupleHelper::WriteNullbits(projected_null_vec, (char*)data);
		return 0;
	}
	return QE_EOF;
}

// For attribute in std::vector<Attribute>, name it as rel.attr
void Project::getAttributes(std::vector<Attribute> &attrs) const{
	attrs = this->project_attrs_;
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
		TableScan *rightIn,           // TableScan Iterator of input S
		const Condition &condition,   // Join condition
		const unsigned numPages // # of pages that can be loaded into memory,
								//   i.e., memory block size (decided by the optimizer)
		) {
	this->outer_ = leftIn;
	this->inner_ = new TableScan(rightIn->rm, rightIn->tableName); // 
	this->condition_ = condition;
	this->buffer_size_ = numPages * PAGE_SIZE;
	this->block_data_ = new char[this->buffer_size_];

	leftIn->getAttributes(out_attrs_);
	rightIn->getAttributes(in_attrs_);

	for (this->out_condition_attrIndex_ = 0; this->out_condition_attrIndex_ < this->out_attrs_.size(); this->out_condition_attrIndex_++) {
		if (strcmp(this->condition_.lhsAttr.c_str(),
				this->out_attrs_[this->out_condition_attrIndex_].name.c_str()) == 0)
			break;
	}

	// do with condition attr type and attr idex
	if (this->condition_.bRhsIsAttr) { // right is attr
		for (this->in_condition_attr_index_ = 0; this->in_condition_attr_index_ < this->in_attrs_.size(); this->in_condition_attr_index_++) {
			if (strcmp(this->condition_.rhsAttr.c_str(), 
				this->in_attrs_[this->in_condition_attr_index_].name.c_str()) == 0)
				break;
		}

		if (this->out_attrs_[this->out_condition_attrIndex_].type != this->in_attrs_[this->in_condition_attr_index_].type) {
			std::cerr << "Data type didnt match! Error!" << std::endl;
			//	condition_attr_type_ = NULL;
		} else {
			condition_attr_type_ = out_attrs_[this->out_condition_attrIndex_].type;
		}
	} else {
		this->in_condition_attr_index_ = UINT_MAX;
		if (this->out_attrs_[this->out_condition_attrIndex_].type
				!= this->condition_.rhsValue.type) {
			std::cerr << "Data type didnt match! Error!" << std::endl;
			//	condition_attr_type_ = NULL;
		} else {
			condition_attr_type_ = out_attrs_[this->out_condition_attrIndex_].type;
		}
	}

	// do with block partition

	this->max_out_recrod_length_ = ceil(this->out_attrs_.size()/8.0);
	for(unsigned i=0;i<this->out_attrs_.size();i++){
		if (this->out_attrs_[i].type == TypeInt || this->out_attrs_[i].type == TypeReal)
			this->max_out_recrod_length_ += sizeof(unsigned);
		else {
			this->max_out_recrod_length_ += this->out_attrs_[i].length;
			this->max_out_recrod_length_ += sizeof(unsigned);
		}
	}

	cur_pos_in_block_ = 0;
	// current_proper_vector_index_ = 0;
	this->LoadBlock();
}


// TO DELETE
BNLJoin::~BNLJoin(){
	delete this->inner_;
	delete[] this->block_data_;
}

RC BNLJoin::getNextTuple(void *data){
	char* out_data_tmp = new char[PAGE_SIZE];
	char* in_data_tmp = new char[PAGE_SIZE];

	unsigned out_data_len = 0;
	while(GetOutTupleViaBlock(out_data_tmp, out_data_len)){	
		int i = (*out_data_tmp);
		// std::cout << i << std::endl;

		Value lhs_value; // remember to free
		lhs_value.data = malloc(kFourBtyesSpace); // will be realloc
		unsigned len_lhs_value;

		TupleHelper::RetrieveMatchedAttribute(out_data_tmp, this->out_attrs_, this->condition_.lhsAttr, lhs_value, len_lhs_value);

		bool outer_comped = false;
		if(!this->condition_.bRhsIsAttr){
			if(TupleHelper::IsComped(lhs_value, this->condition_.rhsValue, this->condition_)){
				outer_comped = true; // comped, do join
			}
			else {
				free(lhs_value.data);
				continue;
			}
		}// else , do attr vs attr compare

		while(this->inner_->getNextTuple(in_data_tmp) != QE_EOF){
			bool is_comped = false;
			if(this->condition_.bRhsIsAttr){ // attr vs attr
				Value rhs_value;
				rhs_value.data = malloc(kFourBtyesSpace); // will be realloc
				unsigned len_rhs_value;
				TupleHelper::RetrieveMatchedAttribute(in_data_tmp, this->in_attrs_, this->condition_.rhsAttr, rhs_value, len_rhs_value);
				is_comped = TupleHelper::IsComped(lhs_value, rhs_value, this->condition_);
				free(rhs_value.data);
				// if(is_comped)
				// 	return 0;
				// else
				// 	continue;
			}
			else{ // attr vs condition
				is_comped = outer_comped;
				// if(is_comped)
				// 	return 0;
				// else
				// 	continue;
			}
			if(is_comped){
				TupleHelper::JoinData(out_data_tmp, this->out_attrs_, in_data_tmp, this->in_attrs_, (char*)data);
				free(lhs_value.data);
				delete[] in_data_tmp;
				delete[] out_data_tmp;
				return 0;
			}
			else {
				continue;
			}

		}

		TableScan* tmp= new TableScan(this->inner_->rm, this->inner_->tableName);
		delete this->inner_;
		this->inner_ = tmp;

		free(lhs_value.data);
	}
	delete[] in_data_tmp;
	delete[] out_data_tmp;
	return QE_EOF;
}

RC TupleHelper::JoinData(const char* out_data_tmp, const std::vector<Attribute>& out_attrs, const char* in_data_tmp, const std::vector<Attribute>& in_attrs, char* data){
	unsigned out_data_length = Record::getRecordContentLength(out_attrs, out_data_tmp);
	unsigned in_data_length = Record::getRecordContentLength(in_attrs, in_data_tmp);

	unsigned out_null_bits_len, in_null_bits_len;
	std::vector<bool> out_null_bool_vec = TupleHelper::CheckNullbits(out_attrs.size(), out_data_tmp, out_null_bits_len);
	std::vector<bool> inner_null_bool_vec = TupleHelper::CheckNullbits(in_attrs.size(), in_data_tmp, in_null_bits_len);
	std::vector<bool> stacked_null_bool_vec;

	// out_vec + in_vec
	stacked_null_bool_vec.reserve(out_null_bool_vec.size() + inner_null_bool_vec.size()); // preallocate memory
	stacked_null_bool_vec.insert(stacked_null_bool_vec.end(), out_null_bool_vec.begin(), out_null_bool_vec.end());
	stacked_null_bool_vec.insert(stacked_null_bool_vec.end(), inner_null_bool_vec.begin(), inner_null_bool_vec.end());

	unsigned data_offset = 0;

	unsigned stacked_null_bits_len = (unsigned)std::ceil((float)stacked_null_bool_vec.size() / 8.0);
	TupleHelper::WriteNullbits(stacked_null_bool_vec, data);
	data_offset += stacked_null_bits_len;

	// memcpy(data + data_offset, out_data_tmp, out_null_bits_len);
	// data_offset += out_null_bits_len;
	// memcpy(data + data_offset, in_data_tmp, in_null_bits_len);
	// data_offset += in_null_bits_len;

	memcpy(data + data_offset, out_data_tmp + out_null_bits_len, out_data_length - out_null_bits_len);
	data_offset += out_data_length - out_null_bits_len;
	memcpy(data + data_offset, in_data_tmp + in_null_bits_len, in_data_length - in_null_bits_len);
	data_offset += in_data_length - in_null_bits_len;

	return 0;
}

// For attribute in std::vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const{
	attrs.reserve(this->out_attrs_.size() + this->in_attrs_.size());
	attrs.insert(attrs.end(), this->out_attrs_.begin(), this->out_attrs_.end());
	attrs.insert(attrs.end(), this->in_attrs_.begin(), this->in_attrs_.end());

	return;
}

bool BNLJoin::GetOutTupleViaBlock(void* data, unsigned& data_length){
	// std::cout << cur_pos_in_block_ << "compare" <<data_length_map_.size() << std::endl;

	if(this->cur_pos_in_block_ >= data_length_map_.size()) // need load block
		if(!this -> LoadBlock())
			return false;

	data_length = data_length_map_[cur_pos_in_block_];
	// data = this->block_data_ + this->max_out_recrod_length_ * this->cur_pos_in_block_;
	memcpy(data, this->block_data_ + this->max_out_recrod_length_ * this->cur_pos_in_block_, data_length);

	++this->cur_pos_in_block_;

	return true;
}

bool BNLJoin::LoadBlock() {
	// load data_length_map_
	// load block_data_
	// reset cur_pos_in_block_
	// delete[] this->block_data_;
	this->data_length_map_.clear();
	// this->block_mapper_.clear();
	// this->block_data_ = new char[this->buffer_size_];

	char* data = new char[PAGE_SIZE];
	unsigned current_offset = 0;

	while(this->outer_->getNextTuple(data) !=  QE_EOF && 
		((this->buffer_size_ - current_offset) > this->max_out_recrod_length_)){
		unsigned data_length = Record::getRecordContentLength(this->out_attrs_, data); // checked nullbits?
		memcpy(this->block_data_ + current_offset, data, data_length);
		int i = *(this->block_data_  + current_offset);
		// std::cout << current_offset << " : " << i << std::endl;
		data_length_map_.push_back(data_length);
		current_offset += this->max_out_recrod_length_;
	}

	this -> cur_pos_in_block_ = 0; // * max_out_recrod_length_
	// this -> currentProperVectorIndex = 0;

	delete[] data;

	if(current_offset == 0){ // no data can be retrieved
		return false;
	}

	return true;
}

// ------------- Index nested-loop join operator ------------- 
INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
       IndexScan *rightIn,          // IndexScan Iterator of input S
       const Condition &condition   // Join condition
){
	this->outer_ = leftIn;
	// new IndexScan(this->inner_->rm, inner_->tableName,inner_->attrName);
	this->inner_ =new IndexScan(rightIn->rm, rightIn->tableName,rightIn->attrName);
	// new TableScan(rightIn->rm, rightIn->tableName); // 
	this->condition_ = condition;

	leftIn->getAttributes(out_attrs_);
	rightIn->getAttributes(in_attrs_);

	for (this->out_condition_attrIndex_ = 0; this->out_condition_attrIndex_ < this->out_attrs_.size(); this->out_condition_attrIndex_++) {
		if (strcmp(this->condition_.lhsAttr.c_str(),
				this->out_attrs_[this->out_condition_attrIndex_].name.c_str()) == 0)
			break;
	}

	// do with condition attr type and attr idex
	if (this->condition_.bRhsIsAttr) { // right is attr
		for (this->in_condition_attr_index_ = 0; this->in_condition_attr_index_ < this->in_attrs_.size(); this->in_condition_attr_index_++) {
			if (strcmp(this->condition_.rhsAttr.c_str(), 
				this->in_attrs_[this->in_condition_attr_index_].name.c_str()) == 0)
				break;
		}

		if (this->out_attrs_[this->out_condition_attrIndex_].type != this->in_attrs_[this->in_condition_attr_index_].type) {
			std::cerr << "Data type didnt match! Error!" << std::endl;
			//	condition_attr_type_ = NULL;
		} else {
			condition_attr_type_ = out_attrs_[this->out_condition_attrIndex_].type;
		}
	} else {
		this->in_condition_attr_index_ = UINT_MAX;
		if (this->out_attrs_[this->out_condition_attrIndex_].type
				!= this->condition_.rhsValue.type) {
			std::cerr << "Data type didnt match! Error!" << std::endl;
			//	condition_attr_type_ = NULL;
		} else {
			condition_attr_type_ = out_attrs_[this->out_condition_attrIndex_].type;
		}
	}

}

// TO DELETE
INLJoin::~INLJoin(){
	delete this->inner_;
}

RC INLJoin::getNextTuple(void *data){
	char* out_data_tmp = new char[PAGE_SIZE];
	char* in_data_tmp = new char[PAGE_SIZE];

	unsigned out_data_len = 0;
	while(this->outer_->getNextTuple(in_data_tmp) != QE_EOF){	
		int i = (*out_data_tmp);
		// std::cout << i << std::endl;

		Value lhs_value; // remember to free
		lhs_value.data = malloc(kFourBtyesSpace); // will be realloc
		unsigned len_lhs_value;

		TupleHelper::RetrieveMatchedAttribute(out_data_tmp, this->out_attrs_, this->condition_.lhsAttr, lhs_value, len_lhs_value);

		bool outer_comped = false;
		if(!this->condition_.bRhsIsAttr){
			if(TupleHelper::IsComped(lhs_value, this->condition_.rhsValue, this->condition_)){
				outer_comped = true; // comped, do join
			}
			else {
				free(lhs_value.data);
				continue;
			}
		}// else , do attr vs attr compare

		while(this->inner_->getNextTuple(in_data_tmp) != QE_EOF){
			bool is_comped = false;
			if(this->condition_.bRhsIsAttr){ // attr vs attr
				Value rhs_value;
				rhs_value.data = malloc(kFourBtyesSpace); // will be realloc
				unsigned len_rhs_value;
				TupleHelper::RetrieveMatchedAttribute(in_data_tmp, this->in_attrs_, this->condition_.rhsAttr, rhs_value, len_rhs_value);
				is_comped = TupleHelper::IsComped(lhs_value, rhs_value, this->condition_);
				free(rhs_value.data);
				// if(is_comped)
				// 	return 0;
				// else
				// 	continue;
			}
			else{ // attr vs condition
				is_comped = outer_comped;
				// if(is_comped)
				// 	return 0;
				// else
				// 	continue;
			}
			if(is_comped){
				TupleHelper::JoinData(out_data_tmp, this->out_attrs_, in_data_tmp, this->in_attrs_, (char*)data);
				free(lhs_value.data);
				delete[] in_data_tmp;
				delete[] out_data_tmp;
				return 0;
			}
			else {
				continue;
			}

		}
		IndexScan* innerTmp = new IndexScan(this->inner_->rm, inner_->tableName,inner_->attrName);
		delete this->inner_;
		this->inner_ = innerTmp;

		// TableScan* tmp= new TableScan(this->inner_->rm, this->inner_->tableName);
		// delete this->inner_;
		// this->inner_ = tmp;

		free(lhs_value.data);
	}
	delete[] in_data_tmp;
	delete[] out_data_tmp;
	return QE_EOF;
}

// For attribute in std::vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(std::vector<Attribute> &attrs) const{
	attrs.reserve(this->out_attrs_.size() + this->in_attrs_.size());
	attrs.insert(attrs.end(), this->out_attrs_.begin(), this->out_attrs_.end());
	attrs.insert(attrs.end(), this->in_attrs_.begin(), this->in_attrs_.end());

	return;
}

// ------------- Grace hash join operator ------------- 
GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
    Iterator *rightIn,               // Iterator of input S
    const Condition &condition,      // Join condition (CompOp is always EQ)
    const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
){

}

// TO DELETE
GHJoin::~GHJoin(){

}

RC GHJoin::getNextTuple(void *data){
	return QE_EOF;
}

// For attribute in std::vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(std::vector<Attribute> &attrs) const{

}

// ------------- Aggregation operator ------------- 
// Mandatory
// ------------- Basic aggregation ------------- 
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
          Attribute aggAttr,        // The attribute over which we are computing an aggregate
          AggregateOp op            // Aggregate operation
):input_(input), agg_op_(op)
{
	this->agg_attr_.name = aggAttr.name;
	this->agg_attr_.type = aggAttr.type;
	this->agg_attr_.length = aggAttr.length;
	aggregate_handle_ = new AggregateHandle(aggAttr, op);

	this->input_->getAttributes(this->attrs_);
	this->is_group_ = false;
}

// Optional for everyone: 5 extra-credit points
// -------------  Group-based hash aggregation ------------- 
Aggregate::Aggregate(Iterator *input,             // Iterator of input R
          Attribute aggAttr,           // The attribute over which we are computing an aggregate
          Attribute groupAttr,         // The attribute over which we are grouping the tuples
          AggregateOp op              // Aggregate operation
):input_(input), agg_op_(op)
{
	this->agg_attr_.name = aggAttr.name;
	this->agg_attr_.type = aggAttr.type;
	this->agg_attr_.length = aggAttr.length;
	aggregate_handle_ = new AggregateHandle(aggAttr, op);

	this->input_->getAttributes(this->attrs_);
	this->is_group_ = true;
}

// TO DELETE
Aggregate::~Aggregate(){
	delete aggregate_handle_;
}

RC Aggregate::getNextTuple(void *data){
	// RC rc = QE_EOF;
	if(!is_group_){
		char* tuple_buf = new char[PAGE_SIZE];
		Value lhs_value;
		lhs_value.data = malloc(kFourBtyesSpace);
		unsigned lhs_len = 0;
		bool is_appended = false; // check if getNext success
		while(this->input_->getNextTuple(tuple_buf) != -1){
			is_appended = true;
			float append_value = 0.f;
			if(TupleHelper::RetrieveMatchedAttribute(tuple_buf, this->attrs_,
									this->agg_attr_.name, lhs_value, lhs_len) == -1){
				continue;
			}

			if(this->agg_attr_.type == TypeVarChar){
#ifdef IAN_DEBUG 
				std::cout << "should not be a string" << std::endl;
#endif
			}
			else if(this->agg_attr_.type == TypeReal){
				memcpy(&append_value, lhs_value.data, kFourBtyesSpace);
			}
			else if(this->agg_attr_.type == TypeInt){
				int temp_int = 0;
				memcpy(&temp_int, lhs_value.data, kFourBtyesSpace);
				append_value = (float)temp_int;
			}

			aggregate_handle_->Append(append_value);


		}
		bool is_valid = this->aggregate_handle_->Result(data); // not used
		free(tuple_buf);
		free(lhs_value.data);
		if(is_appended)
			return 0;
		else return QE_EOF;
	}
	return QE_EOF;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(std::vector<Attribute> &attrs) const{
	Attribute packed_attr;
	packed_attr.type = this->agg_attr_.type;
	packed_attr.length = this->agg_attr_.length;

    // std::string attribute_name;
    std::string op_name;
    std::string attr_name;

    switch (this->agg_op_){
        case MIN:
            op_name = "MIN";
            break;
        case MAX:
            op_name = "MAX";
            break;
        case SUM:
            op_name = "SUM";
            break;
        case AVG:
            op_name = "AVG";
            break;
        case COUNT:
            op_name = "COUNT";
            break;
    }

    attr_name = "(" + this->agg_attr_.name + ")";

    packed_attr.name = op_name + attr_name;
    attrs.push_back(packed_attr);

    return;
}

AggregateHandle::AggregateHandle(const Attribute& agg_attr, const AggregateOp& agg_op)
								: agg_op_(agg_op), count_(0){
	this->agg_attr_.name = agg_attr.name;
	this->agg_attr_.type = agg_attr.type;
	this->agg_attr_.length = agg_attr.length;
	switch (this->agg_op_){
	    case MIN:
		    float_value_ = std::numeric_limits<float>::max();
	    	break;
	    case MAX:
			float_value_ = std::numeric_limits<float>::min();
			break;
		default: // sum, avg, count
			float_value_ = 0.f;
			break;
	}
}

void AggregateHandle::Append(float append_value){
	++this->count_;
	switch (this->agg_op_){
	    case MIN:
		    float_value_ = append_value < float_value_ ? append_value : float_value_;
	    	break;
	    case MAX:
		    float_value_ = append_value > float_value_ ? append_value : float_value_;
			break;
		case SUM:
			float_value_ += append_value;
			break;
		case AVG:
            float_value_ = ((float_value_ * (count_ - 1)) + append_value) / count_;		
			break;
		case COUNT:
			break;
		default: // sum, avg, count
			float_value_ = this->count_;
			break;
	}

}

bool AggregateHandle::Result(void* data){
	// WARNING:nullbits
	char nullbits = 0; // 1 byte fixed
	memcpy(data, &nullbits, sizeof(char));
    memcpy(data + sizeof(char), &this->float_value_, sizeof(float));

	switch (this->agg_op_){
	    case MIN:
		    if(this->float_value_ == std::numeric_limits<float>::max())
		    	return false;
	    	break;
	    case MAX:
			if(this->float_value_ == std::numeric_limits<float>::min())
				return false;
			break;
		default:
			break;
		}

    return true;
}
