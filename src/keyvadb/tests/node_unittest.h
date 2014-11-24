#include "tests/common.h"
#include "db/node.h"

using namespace keyvadb;

template <typename T>
class NodeTest : public ::testing::Test
{
   public:
    T policy_;
};

typedef ::testing::Types<detail::KeyUtil<256>> NodeTypes;
TYPED_TEST_CASE(NodeTest, NodeTypes);

TYPED_TEST(NodeTest, Node)
{
    auto first = this->policy_.MakeKey(1);
    auto last = this->policy_.FromHex('F');
    ASSERT_THROW((Node<256>(0, 10, 84, last, first)), std::domain_error);
    Node<256> node(0, 10, 84, first, last);
    ASSERT_TRUE(node.IsSane());
    ASSERT_EQ(10UL, node.Level());
    ASSERT_EQ(84UL, node.Degree());
    ASSERT_EQ(84UL, node.EmptyChildCount());
    ASSERT_EQ(83UL, node.MaxKeys());
    ASSERT_EQ(83UL, node.EmptyKeyCount());
    node.AddSyntheticKeyValues();
    ASSERT_TRUE(node.IsSane());
}

TYPED_TEST(NodeTest, CopyAssign)
{
    auto first = this->policy_.MakeKey(1);
    auto last = this->policy_.FromHex('F');
    auto middle = this->policy_.FromHex(
        "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF9");
    Node<256> node(0, 10, 16, first, last);
    ASSERT_TRUE(node.IsSane());
    auto copyNode = Node<256>(node);
    ASSERT_TRUE(copyNode.IsSane());
    node.AddSyntheticKeyValues();
    node.SetChild(0, 1);
    ASSERT_NE(node.GetKeyValue(7), copyNode.GetKeyValue(7));
    ASSERT_NE(node.GetChild(0), copyNode.GetChild(0));
    auto assignNode = copyNode;
    assignNode.AddSyntheticKeyValues();
    node.SetChild(0, 2);
    ASSERT_EQ(middle, node.GetKeyValue(7).key);
    ASSERT_NE(assignNode.GetKeyValue(7), copyNode.GetKeyValue(7));
    ASSERT_NE(node.GetChild(0), copyNode.GetChild(0));
}

TEST(NodeTest, CalculateDegree)
{
    ASSERT_EQ(77UL, Node<256>::CalculateDegree(4096));
    ASSERT_EQ(156UL, Node<256>::CalculateDegree(8192));
}