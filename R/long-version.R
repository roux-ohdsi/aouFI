
############# 1000 ############
n = 1000

start = Sys.time()
## LARRYS CODE
# Divide data into train and test sets
train <- sample(1:n, 0.5*n)
test <- -train

# Train on a portion of the data
# The argument `depth` controls the depth of the tree.
# Depth k means that we can partition the data into up to 2^(k+1) regions.
policy <- policy_tree(X[train,], gamma.matrix[train,], depth=2)

# Predict on remaining portion
# Note policytree recodes the treatments to 1,2
# We substract one to get back to our usual encoding 0,1.
pi.hat <- predict(policy, X[test,]) - 1

## END LARRYS CODE
end = Sys.time()

elapsed_10000 = end-start


############# 10000 ############
n = 10000

start = Sys.time()
## LARRYS CODE
# Divide data into train and test sets
train <- sample(1:n, 0.5*n)
test <- -train

# Train on a portion of the data
# The argument `depth` controls the depth of the tree.
# Depth k means that we can partition the data into up to 2^(k+1) regions.
policy <- policy_tree(X[train,], gamma.matrix[train,], depth=2)

# Predict on remaining portion
# Note policytree recodes the treatments to 1,2
# We substract one to get back to our usual encoding 0,1.
pi.hat <- predict(policy, X[test,]) - 1

## END LARRYS CODE
end = Sys.time()

elapsed_1000 = end-start

############# 100000 ############
n = 100000

start = Sys.time()
## LARRYS CODE
# Divide data into train and test sets
train <- sample(1:n, 0.5*n)
test <- -train

# Train on a portion of the data
# The argument `depth` controls the depth of the tree.
# Depth k means that we can partition the data into up to 2^(k+1) regions.
policy <- policy_tree(X[train,], gamma.matrix[train,], depth=2)

# Predict on remaining portion
# Note policytree recodes the treatments to 1,2
# We substract one to get back to our usual encoding 0,1.
pi.hat <- predict(policy, X[test,]) - 1

## END LARRYS CODE
end = Sys.time()

elapsed_100000 = end-start
