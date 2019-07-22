# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import numpy as np


class ConsineMatrix:
    def __init__(self):
        pass

    def cosine(_vec1, _vec2):
        ### 向量计算余弦相似度
        ### 计算两向量余弦相似度
        ### 返回_vec1和_vec2余弦相似度
        ### 此处用于计算向量余弦相似度，验证矩阵相似度计算结果
        import numpy
        return float(numpy.sum(_vec1 * _vec2)) / (numpy.linalg.norm(_vec1) * numpy.linalg.norm(_vec2))

    def cosine_Matrix(_matrixA, _matrixB):
        ### 矩阵矢量化操作
        ### 按行计算余弦相似度
        ### 两矩阵计算相似度向量应为同维度
        ### 返回值RES为A矩阵每行对B矩阵每行向量余弦值
        ### RES[i,j] 表示A矩阵第i行向量与B矩阵第j行向量余弦相似度
        import numpy
        _matrixA_matrixB = _matrixA * _matrixB.transpose()
        ### 按行求和，生成一个列向量
        ### 即各行向量的模
        _matrixA_norm = numpy.sqrt(numpy.multiply(_matrixA, _matrixA).sum(axis=1))
        _matrixB_norm = numpy.sqrt(numpy.multiply(_matrixB, _matrixB).sum(axis=1))
        return numpy.divide(_matrixA_matrixB, _matrixA_norm * _matrixB_norm.transpose())

    def main(self):
        pass
